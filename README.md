# Floki

[![Build Status](https://travis-ci.org/arthurprs/floki.svg)](https://travis-ci.org/arthurprs/floki)
[![Coverage Status](https://coveralls.io/repos/arthurprs/floki/badge.svg?branch=master&service=github)](https://coveralls.io/github/arthurprs/floki?branch=master)

*Although it works fairly well please do not trust it for any kind of production data*

Floki borrows concepts from both Kafka and Amazon SQS into an easy to use package. Queues have independent channels which allow efficient fanout and are persisted to disk. Clients talk to it using Redis protocol and commands, so all Redis existing clients can be used. Although there's no replication or clustering support and it might never have.

The design is based on a single thread to handle all networking using an async io but the actual commands are dispatched to a thread pool for execution. State is kept in-memory and checkpointed to disk periodically, queue storage is based on a Write Ahead Log splited into segments.

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
- [x] Map Floki commands to Redis commands in a sensible way
- [x] Better error handling
- [ ] Lots of documentation
- [ ] Opt-in persistency guarantee (needs group commit-ish implementation)
- [ ] Make internal data structures size-bounded, based on configurations

# Floki Protocol (Redis)

How Floki maps Redis commands. This is a work in progress and constantly changing.

**RPUSH**  push one or more messages

```RPUSH queue_name message1 [message2, ...]```

Returns the number of messages inserted

**HMGET** get one or more messages

```HMGET queue_name channel_name number_or_messages [long_pooling_timeout]```

Returns an array with three items for each message. The first would be the id, the second is the ticket (for acknowledging, see bellow) and the third would be the message itself. 
Note: Floki will return as soon as there's one message available

**HMSET** seeks the channel

```HMGET queue_name channel_name TS|ID seek_timestamp|seek_id```

Seeks the specified channel to the specified id or timestamp.

Note: Floki won't error if the *id* or *timestamp* is either in the future or is already gone from the underlying storage. If it's set to a non-existent past, gets will just return the first available message.

**HDEL** ack messages

```HDEL queue_name channel_name ticket1 [ticket2, ...]```

Acknowledge messages using their tickets. Returns the number of successful acknowledges.

**SET** create queue/channel

```SET queue_name channel_name [NX]```

```SETNX queue_name channel_name```

Creates the specified queue and channel, will error if the queue or channel already exists unless the nx flag or the setnx commands is used.

**DEL** delete queue/channel

```DEL queue_name [channel_name]```

Deletes a channel, if specified, otherwise deletes the queue.

**SREM** purge queue/channel

```SREM queue_name channel_name_or_*```

Note: you can also use * as the channel name to purge all channels, effectively purging the entire queue and allowing all used disk space to be reclaimed.

**INFO** get information

```INFO [server|queues|queues.queue_prefix]```

All variants return an array with a single json encoded object.

`server` return information about the server

`queues` return information about all queues

`queues.queue_prefix` return information about all queues matching the given prefix

**CONFIG GET** get configuration

```CONFIG GET [server|queues.queue_name]```

All variants return an array with a single json encoded object.

`server` return the server configuration

`queues.queue_name` return the specified queue configuration

**CONFIG SET** set configuration

```CONFIG SET [server|queues.queue_name].config_name config_value```

Like the previous but to change configuration values.

`config_name` and `config_value` accept the same format as their corresponding entry in the [floki.toml](floki.toml) file, check it's contents for documentation about which values can be changed at runtime.

# Example

See example [helloworld.rs](examples/helloworld.rs)

# Configuration

See the configuration file [floki.toml](floki.toml)

# Running

You'll need a recent version of rust nightly and cargo installed.

On the root of the repostory type:

```RUST_BACKTRACE=1 RUST_LOG=floki=info cargo run --release```

You can also cargo install it using `cargo install --path .` then `RUST_BACKTRACE=1 RUST_LOG=floki=info floki`

# Copyright and License

Code released under the [MIT license](LICENSE).
