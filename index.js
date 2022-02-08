const express = require("express");
const axios = require("axios");
const Queue = require("./queue");

const queue = new Queue({
  connectionConfig: {
    host: "localhost", port: 5672, user: "guest", pass: "guest"
  },
  exchange: "messages",
  exchangeType: "topic"
});

const app = express();

function readJsonBody(options) {
  return (req, res, next) => express.json(options)(req, res, next);
}

async function initServer() {
  console.log("connecting to queue");
  await queue.connect();
  console.log("queue connected");

  app.use("/", readJsonBody({ limit: "100kb" }));

  app.get("/statistics", async (req, res) => {
    console.log(`new request to get stats`);
    try {
      const handlerRes = await axios.get("http://localhost:8001/statistics", { responseType: "stream" });
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

  app.listen(8000, () => {
    console.log(`api listening on port 8000`);
  });  
}

initServer();
