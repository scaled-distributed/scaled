import argparse
import curses
import functools
import json
import logging
from typing import Callable
from typing import List
from typing import Literal

import zmq

from scaled.protocol.python.message import MessageType
from scaled.protocol.python.message import PROTOCOL
from scaled.protocol.python.message import SchedulerState
from scaled.utility.formatter import format_bytes
from scaled.utility.formatter import format_integer
from scaled.utility.formatter import format_microseconds
from scaled.utility.formatter import format_percentage
from scaled.utility.formatter import format_seconds
from scaled.utility.zmq_config import ZMQConfig

SORT_BY_OPTIONS = {
    ord("n"): "worker",
    ord("C"): "agt_cpu",
    ord("M"): "agt_rss",
    ord("c"): "cpu",
    ord("m"): "rss",
    ord("f"): "free",
    ord("w"): "sent",
    ord("d"): "queued",
    ord("l"): "lag",
}


def get_args():
    parser = argparse.ArgumentParser(
        "poke scheduler for monitoring information", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--timeout", type=int, default=5, help="timeout seconds")
    parser.add_argument("address", help="scheduler address to connect to")
    return parser.parse_args()


def main():
    curses.wrapper(poke)


def poke(screen):
    args = get_args()

    screen.nodelay(1)
    config = {"sort_by": "cpu"}

    try:
        subscribe_status(
            address=ZMQConfig.from_string(args.address),
            callback=functools.partial(show_status, screen=screen, config=config),
            timeout=args.timeout,
        )
    except zmq.Again:
        raise TimeoutError(f"Cannot connect to monitoring address {args.address} after {args.timeout} seconds")
    except KeyboardInterrupt:
        pass


def subscribe_status(address: ZMQConfig, callback: Callable[[SchedulerState], None], timeout: int):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVTIMEO, timeout * 1000)
    socket.connect(address.to_address())
    socket.subscribe("")

    while frames := socket.recv_multipart():
        if len(frames) < 2:
            logging.error(f"received unexpected frames {frames}")
            continue

        if frames[0] not in {member.value for member in MessageType}:
            logging.error(f"received unexpected message type: {frames[0]}")
            return

        message_type_bytes, *payload = frames
        message_type = MessageType(message_type_bytes)
        message = PROTOCOL[message_type].deserialize(payload)

        if message_type != message_type.SchedulerState:
            continue

        assert isinstance(message, SchedulerState)
        callback(message)


def show_status(status: SchedulerState, screen, config):
    data = json.loads(status.data)

    option = screen.getch()
    if option in SORT_BY_OPTIONS:
        config["sort_by"] = SORT_BY_OPTIONS[option]

    data["scheduler"]["cpu"] = format_percentage(data["scheduler"]["cpu"])
    data["scheduler"]["rss"] = format_bytes(data["scheduler"]["rss"])
    scheduler_table = __generate_keyword_data("scheduler", data["scheduler"])
    task_manager_table = __generate_keyword_data("task_manager", data["task_manager"], format_integer_flag=True)
    sent_table = __generate_keyword_data("scheduler_sent", data["binder"]["sent"], format_integer_flag=True)
    received_table = __generate_keyword_data("scheduler_received", data["binder"]["received"], format_integer_flag=True)
    client_table = __generate_keyword_data("client_manager", data["client_manager"], key_col_length=18)
    function_name_to_tasks = __generate_keyword_data("func_to_num_tasks", data["function_manager"], key_col_length=18)
    worker_manager_table = __generate_worker_manager_table(
        data["worker_manager"], worker_length=24, sort_by=config["sort_by"]
    )

    table1 = __merge_tables(scheduler_table, task_manager_table, padding="|")
    table1 = __merge_tables(table1, sent_table, padding="|")
    table1 = __merge_tables(table1, received_table, padding="|")

    table2 = __concat_tables(client_table, function_name_to_tasks)
    table3 = __merge_tables(worker_manager_table, table2, padding="|")

    screen.clear()
    try:
        new_row, max_cols = __print_table(screen, 0, table1, padding=1)
    except curses.error:
        __print_too_small(screen)
        return

    try:
        screen.addstr(new_row, 0, "-" * max_cols)
        screen.addstr(new_row + 1, 0, "Shortcuts: " + " ".join([f"{v}[{chr(k)}]" for k, v in SORT_BY_OPTIONS.items()]))
        screen.addstr(new_row + 3, 0, f"Total {len(data['worker_manager'])} worker(s)")
        _ = __print_table(screen, new_row + 4, table3)
    except curses.error:
        pass

    screen.refresh()


def __generate_keyword_data(title, data, key_col_length: int = 0, format_integer_flag: bool = False):
    table = [[title, ""]]

    def format_integer_func(value):
        if format_integer_flag:
            return format_integer(value)

        return value

    table.extend([[__truncate(k, key_col_length), format_integer_func(v)] for k, v in data.items()])
    return table


def __generate_worker_manager_table(wm_data, worker_length: int, sort_by: str):
    if not wm_data:
        headers = [["No workers"]]
        return headers

    wm_data = sorted(wm_data, key=lambda item: item[sort_by], reverse=True)

    for row in wm_data:
        row["worker"] = __truncate(row["worker"], worker_length, how="left")
        row["agt_cpu"] = format_percentage(row["agt_cpu"])
        row["agt_rss"] = format_bytes(row["agt_rss"])
        row["cpu"] = format_percentage(row["cpu"])
        row["rss"] = format_bytes(row["rss"])

        last = row.pop("last")
        last = f"({format_seconds(last)}) " if last > 5 else ""
        row["lag"] = last + format_microseconds(row["lag"])

    worker_manager_table = [[f"[{v}]" if v == sort_by else v for v in wm_data[0].keys()]]
    worker_manager_table.extend([list(worker.values()) for worker in wm_data])
    return worker_manager_table


def __print_table(screen, line_number, data, padding: int = 1):
    if not data:
        return

    col_widths = [max(len(str(row[i])) for row in data) for i in range(len(data[0]))]

    for i, header in enumerate(data[0]):
        screen.addstr(line_number, sum(col_widths[:i]) + (padding * i), str(header).rjust(col_widths[i]))

    for i, row in enumerate(data[1:], start=1):
        for j, cell in enumerate(row):
            screen.addstr(line_number + i, sum(col_widths[:j]) + (padding * j), str(cell).rjust(col_widths[j]))

    return line_number + len(data), sum(col_widths) + (padding * len(col_widths))


def __merge_tables(left: List[List], right: List[List], padding: str = "") -> List[List]:
    if not left:
        return right

    if not right:
        return left

    result = []
    for i in range(max(len(left), len(right))):
        if i < len(left):
            left_row = left[i]
        else:
            left_row = [""] * len(left[0])

        if i < len(right):
            right_row = right[i]
        else:
            right_row = [""] * len(right[0])

        if padding:
            padding_column = [padding]
            result.append(left_row + padding_column + right_row)
        else:
            result.append(left_row + right_row)

    return result


def __concat_tables(up: List[List], down: List[List], padding: int = 1) -> List[List]:
    max_cols = max([len(row) for row in up] + [len(row) for row in down])
    for row in up:
        row.extend([""] * (max_cols - len(row)))

    padding_rows = [[""] * max_cols] * padding

    for row in down:
        row.extend([""] * (max_cols - len(row)))

    return up + padding_rows + down


def __truncate(string: str, number: int, how: Literal["left", "right"] = "left") -> str:
    if number <= 0:
        return string

    if len(string) <= number:
        return string

    if how == "left":
        return f"{string[:number]}+"
    else:
        return f"+{string[-number:]}"


def __print_too_small(screen):
    screen.clear()
    screen.addstr(0, 0, "Your terminal is too small to show")
    screen.refresh()
