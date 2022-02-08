
const amqp = require("amqplib");

class Queue {
  constructor({ connectionConfig, exchange, exchangeType, prefetch }) {
    this.connectionConfig = connectionConfig;
    this.exchange = exchange;
    this.exchangeType = exchangeType || "direct";
    this.prefetch = prefetch || 10;
  }

  async connect() {
    return this._connect();
  }

  async assertQueue({ queueName, durable = true, queueArguments = {} }) {
    await this._connect();
    await this._createChannel();
    await this.channel.assertExchange(this.exchange, this.exchangeType);
    await this.channel.assertQueue(queueName, { durable, arguments: queueArguments });
    await this.channel.bindQueue(queueName, this.exchange, this.bindPattern || queueName);
  }

  async subscribe({
    queueName,
    eventHandler,
    durable = true,
    queueArguments = {},
    requeue = true
  }) {
    await this._connect();
    try {
      console.log(
        this._getAmqpConnectionData(),
        `subscribing to queue ${queueName}, ${typeof eventHandler}`
      );
      await this._createChannel();
      await this.channel.assertExchange(this.exchange, this.exchangeType);
      this.channel.prefetch(this.prefetch);
      await this.channel.assertQueue(queueName, { durable, arguments: queueArguments });
      await this.channel.bindQueue(queueName, this.exchange, queueName);
      await this.channel.consume(queueName, async (msg) => {
        await this._onEvent(eventHandler, msg, queueName, requeue);
      });
      console.log(
        this._getAmqpConnectionData(),
        `subscribed to queue ${queueName} with subscription`
      );
    } catch (e) {
      console.error(
        { exception: e, ...this._getAmqpConnectionData() },
        `error subscribng to queue: ${queueName}`
      );
    }
  }

  async publish({ routingKey, attributes, message }) {
    if (!(await this._connect())) {
      throw new Error("can't publish while connection is down");
    }
    const payload = this._getContentDataToPublish(attributes, message);
    await this._createChannel();
    await this.channel.publish(this.exchange, routingKey, Buffer.from(JSON.stringify(payload)));
    console.trace(this._getAmqpConnectionData(), `published message to routingKey ${routingKey}`);
  }

  async _connect() {
    if (this.connection) return true;
    
    console.log("connecting to queue");
    const { host, port, user, pass, heartbeat } = this.connectionConfig;
    try {
      this.connection = await amqp.connect({
        hostname: host,
        port,
        username: user,
        password: pass,
        heartbeat
      });
      this.connection.on("error", (e) => this._handleConnectionError(e, "connection error"));
      console.log(this._getAmqpConnectionData(), "queue connection established");
      return true;
    } catch (e) {
      this._handleConnectionError(e, "error while trying to connect");
      return false;
    }
  }

  async _createChannel() {
    if (this.channel) return;

    this.channel = await this.connection.createChannel();
    this.channel.on("error", (e) => this._handleConnectionError(e, "channel error"));
    this.channel.on("close", (e) => this._handleConnectionError(e, "channel closed"));
  }

  async close() {
    await this._close();
  }

  _handleConnectionError(msg) {
    console.log(`should reconnect due to: ${msg}`);
  }

  async _onEvent(eventHandler, msg, queueName, requeue) {
    try {
      console.trace(this._getAmqpConnectionData(), `received message to ${queueName}`);
      await eventHandler(msg);
      await this.channel.ack(msg);
    } catch (e) {
      console.error(
        { ...this._getAmqpConnectionData(), exception: e },
        `error while processing message to ${queueName}`
      );
      await this.channel.nack(msg, false, requeue);
    }
  }

  async _close() {
    try {
      await this.channel.close();
    } catch (err) {}
    this.channel = undefined;
    try {
      await this.connection.close();
    } catch (err) {}
    this.connection = undefined;
  }

  _getContentDataToPublish(attributes, message) {
    attributes = { ...attributes, ...{} };
    return { attributes, data: message };
  }

  _getAmqpConnectionData() {
    return {
      exchange: this.exchange
    };
  }
}

module.exports = Queue;