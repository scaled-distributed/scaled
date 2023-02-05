# Scaled
This project is aiming the target that provides simple and efficient and reliable way for distributing computing 
framework, centralized scheduler and stable protocol when client and worker talking to scheduler

# Introduction
The goal for this project should be as simple as possible
- It built on top of zmq
- it has ready python version of Client, Scheduler, Worker
- I will provide golang or Rust version of Scheduler, the goal for the Scheduler should be completely computer language 
  agnostic, which means they follow the same protocol
- Scheduler might support function based computing tree in the future