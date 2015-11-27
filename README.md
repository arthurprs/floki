# Floki

[![Build Status](https://travis-ci.org/arthurprs/floki.svg)](https://travis-ci.org/arthurprs/floki)
[![Coverage Status](https://coveralls.io/repos/arthurprs/floki/badge.svg?branch=master&service=github)](https://coveralls.io/github/arthurprs/floki?branch=master)

**Atention, this is a work in progress and completely unusable at this point**

Floki borrows concepts from both Kafka and Amazon SQS into an easy to use package. Queues have independent channels and are fully persisted to disk. Clients talk to it using Redis protocol and commands, so all Redis exiting clients can be used.

## Goals
- [x] Redis protocol
- [x] Multiple Queues
- [x] Multiple Channels per Queue (efficient fanout)
- [x] Disk Backed
- [x] Runs on most Unix systems
- [x] Crash resistant
- [ ] Hardened internals

## TODO
- [x] Ticket based Acknowledgement
- [ ] Map commands to Redis commands in a sensible way
- [ ] Better error handling (wip)
- [ ] Lots of documentation
- [ ] Opt-in persistency guarantee (needs group commit-ish implementation)
- [ ] Make internal data structures size-bounded, based on configurations
