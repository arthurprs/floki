# Floki

[![Build Status](https://travis-ci.org/arthurprs/floki.svg)](https://travis-ci.org/arthurprs/floki)
[![Coverage Status](https://coveralls.io/repos/arthurprs/floki/badge.svg?branch=master&service=github)](https://coveralls.io/github/arthurprs/floki?branch=master)

**Atention, this is a work in progress and probably unusable at this point**

Floki borrows concepts from both Kafka and Amazon SQS into an easy to use package. Queues have independent channels which allow efficient fanout and are persisted to disk. Clients talk to it using Redis protocol and commands, so all Redis exiting clients can be used. Althought there's no replication or clustering support and it might never have.

The design is based on a single thread to handle all networking using an async io but the actual commands are dispatched to a thread pool for execution. State is kept in-memory and checkpointed to disk periodically, queue storage is based on a Whrite Ahead Log splited into segments.

I started this project in order to evaluate Rust capabilities in a real world project which I thought it could shine and once you get used to the rust way it indeed does.

## Goals
- [x] Redis protocol
- [x] Multiple Queues
- [x] Multiple Channels per Queue (for efficient fanout)
- [x] Disk Backed
- [x] Runs on most Unix systems
- [x] Crash resistant
- [x] Soft and Hard retention periods
- [x] Soft and Hard retention sizes
- [x] Channel seek, with either ID or timestamp

## TODO
- [x] Ticket based Acknowledgement
- [ ] Map Floki commands to Redis commands in a sensible way
- [x] Better error handling
- [ ] Lots of documentation
- [ ] Opt-in persistency guarantee (needs group commit-ish implementation)
- [ ] Make internal data structures size-bounded, based on configurations

# Copyright and License

Code released under the [MIT license](LICENSE).
