###############################################################################
# Consumes one or more AMQP Queues and saves messages to MongoDB collection
###############################################################################

_ = require("lodash")
async = require("async")
AMQP = require("amqplib/callback_api")
MongoClient = require("mongodb").MongoClient

CONFIG = require("convig").env({
  AMQPHOST: "amqp://guest:guest@localhost"
  MONGODB: "mongodb://localhost/amqp"
  MONGOCOLLECTION: "messages"
  TRANSLATECONTENT: true
  REQUEUEERRORS: false
})

if not (queueNames = process.argv.slice(2)).length
  console.error("Queue names needed as arguments")
  process.exit(1)

async.auto {
  # Setup the AMQP Channel to Consume Queues
  amqp: (next, auto) -> AMQP.connect(CONFIG.AMQPHOST, next)
  channel: ["amqp", (next, auto) ->
    auto.amqp.createChannel(next)
  ]
  # Setup MongoDB Client, Collection
  db: (next, auto) -> MongoClient.connect(CONFIG.MONGODB, next)
  collection: ["db", (next, auto) ->
    auto.db.collection(CONFIG.MONGOCOLLECTION, next)
  ]
  # Setup subscription methods when both AMQP channel/MongoDB are ready
  subscriptions: ["collection", "channel", (next, auto) ->
    tc = CONFIG.TRANSLATECONTENT
    subscribe = (queue, next) ->
      saver = new MessageSaver(auto.collection, auto.channel, queue, tc)
      auto.channel.consume(queue, saver.save, {}, next)
    async.map(queueNames, subscribe, next)
  ]
}, (err, results) ->
  if err
    console.error(err)
    return process.exit(1)
  # Output Information about target
  console.error(
    "[%s] Saving to '%s' in '%s'",
    new Date(),
    CONFIG.MONGOCOLLECTION,
    CONFIG.MONGODB
  )
  # Output Information about consumption
  for queue, ix in queueNames
    cTag = results.subscriptions[ix].consumerTag
    console.error("[%s] Consuming Queue: %s (cTag=%s)", new Date(), queue, cTag)

# Helper object to save incoming messages via AMQP callback API
class MessageSaver
  constructor: (@collection, @channel, @queueName, transContent) ->
    @save = (msg) =>
      # Attach all message stuff sans content
      toSave = _.assign({date: new Date(), queue: @queueName}, {
        fields: _.pick(msg.fields, (v) -> v?)
        properties: _.pick(msg.properties, (v) -> v?)
      })
      # Strip empty headers
      if _.isEmpty(toSave.properties.headers)
        delete toSave.properties.headers
      # Content Translation
      if transContent
        toSave.content = convertContent(msg.content, toSave.properties)
        finalTranslate(toSave)
      else
        toSave.content = msg.content
      # Save docment
      @collection.insert toSave, (err, doc) => @handleError(err, msg, doc)
  # Dump out any error messages, always acknowledge
  handleError: (err, msg, doc) ->
    return @channel.ack(msg) if not err
    console.error(err)
    console.error("[%s] Failed to save: %s", new Date(), JSON.stringify(doc))
    @channel.reject(msg, REQUEUEERRORS)

# Serializes message for JSON output
finalTranslate = (base) ->
  # Alter object content based on type
  cType = base.properties.contentType
  if /^application\/json$/.test(cType)
    try
      base.content = JSON.parse(base.content)
    catch e
      base.properties.contentType = "application/octet-stream"
      base.error = _.pick(e, "message", "stack", "code")
  else if not /^text\//.test(cType)
    base.properties.contentType ?= "application/octet-stream"
  return base

# Converts Buffer to a string, altering contentEncoding as appropriate
convertContent = (buff, props) ->
  cEnc = props.contentEncoding?.toLowerCase().replace(/[^a-z0-9]/g, "")
  # For JSON content without encoding, assume UTF-8
  if not cEnc and /^application\/json/.test(props.contentType)
    props.contentEncoding = "utf8"
    return buff.toString()
  # Leave hex strings as HEX-viewable
  if cEnc is "hex"
    props.contentEncoding = "hex"
    return buff.toString("ascii")
  # ASCII text can keep its encoding
  if cEnc is "ascii"
    props.contentEncoding = "ascii"
    return buff.toString("ascii")
  if cEnc is "utf8"
    props.contentEncoding = "utf8"
    return buff.toString("utf8")
  # Convert UCS-2, it will be output as UTF-8
  if cEnc in ["utf16le", "ucs2"]
    props.contentEncoding = "utf8"
    return buff.toString(cEnc)
  # Binary: convert to base64
  if cEnc is "binary"
    props.contentEncoding = "base64"
    return buff.toString("base64")
  # Already base64: don't double-encode
  if cEnc is "base64"
    props.contentEncoding = "base64"
    return buff.toString("ascii")
  # Leave a trace of other unknown encoding before base64
  props.contentEncoding = if cEnc then "base64,#{cEnc}" else "base64"
  return buff.toString("base64")
