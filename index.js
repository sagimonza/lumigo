const express = require("express");
const axios = require("axios");
const Queue = require("./queue");

const queue = new Queue({
  connectionConfig: {
    host: process.env.QUEUE_HOST, port: process.env.QUEUE_PORT, user: "guest", pass: "guest"
  },
  exchange: "messages",
  exchangeType: "topic"
});

const app = express();

function readJsonBody(options) {
  return (req, res, next) => express.json(options)(req, res, next);
}

async function wait(timeMs) {
  return new Promise((resolve) => setTimeout(resolve, timeMs));
}

async function initServer() {
  // wait for queue to be ready
  await wait(1000 * 15);

  console.log("connecting to queue");
  await queue.connect();
  console.log("queue connected");

  app.use("/", readJsonBody({ limit: "100kb" }));

  app.get("/statistics", async (req, res) => {
    console.log(`new request to get stats`);
    try {
      const handlerRes = await axios.get(`http://${process.env.HANDLER_HOST}:${process.env.HANDLER_PORT}/statistics`, { responseType: "stream" });
      handlerRes.data.pipe(res);
    } catch (ex) {
      console.log(ex);
    }
  });

  // validate msg, handle errors
  app.post("/messages", async (req, res) => {
    console.log(`new request to dispatch function`);
    if (!req.body?.message) return res.status(400).json({ error: "missing message property on request body" });

    await queue.publish({ routingKey: "invocations", attributes: {}, message: req.body });
    res.status(200).json({ data: "message dispatched" });
  });

  app.listen(process.env.APP_PORT, () => {
    console.log(`api listening on port ${process.env.APP_PORT}`);
  });  
}

initServer();
