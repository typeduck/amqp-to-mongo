# AMQP to MongoDB

amqp-to-mongo consumes AMQP messages from one or more queues, and saves them to
a MongoDB collection.


## Usage and Configuration

Queue names to consume are given on the command line.

```sh
amqp-to-mongo queue-name [queue-name...]
```

Other behaviour is affected via the following environment variables:

- **AMQPHOST**: AMQP Server URL (default: amqp://guest:guest@localhost)
- **MONGODB**: MongoDB Database URL (default: mongodb://localhost/amqp)
- **MONGOCOLLECTION**: Database collection to save to (default: messages)
- **TRANSLATECONTENT**: translate content field (default: true)

If messages fail to be saved to MongoDB, they will be rejected (but NOT
requeued).


## Data Structures

The data structures are saved as consumed by
[amqplib](http://www.squaremobius.net/amqp.node/doc/channel_api.html), with a
little extra:

- *date*: new Date instance at moment of consumption
- *queue*: name of AMQP Queue on which message arrived
- *fields*: non-empty (null/undefined) values from amqplib
- *properties*: non-empty (null/undefined) values from amqplib
- *content*: content from amqplib, *possibly translated*

The *date* is intended to allow you to set a
[TTL index](http://docs.mongodb.org/manual/core/index-ttl/), or just to keep
track of when the message was consumed (although the ObjectId can do this,
too).

The *queue* is added so you can remember where the object came from, although
routing info is also included in the *fields* from amqplib.


## Content Translation

Content translation of the *content* field occurs prior to saving unless
**TRANSLATECONTENT** is turned off. When turned off, the messages are saved as
binary types.

If the AMQP properties content-type and/or content-encoding are specified, the
content will be translated.

- *application/json* is parsed into JSON object
- *text/\** is read as a string
- *everything else* is saved as a string, encoded in ascii, base64, or utf-8,
  depending on content-encoding and whether possible to convert to utf-8
