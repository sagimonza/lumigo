const express = require("express");
const Queue = require("./queue");
const FunctionsManager = require("./functionsManager");

const queue = new Queue({
  connectionConfig: {
    host: "localhost", port: 5672, user: "guest", pass: "guest"
  },
  exchange: "messages",
  exchangeType: "topic"
});
const functionsManager = new FunctionsManager();
const app = express();

async function handleInvocation(msg) {
  const msgObj = JSON.parse(msg.content.toString());
  functionsManager.dispatchFunction(msgObj.data.message);
}

async function initHandler() {
  await queue.subscribe({ queueName: "invocations", eventHandler: handleInvocation });

  function readJsonBody(options) {
    return (req, res, next) => express.json(options)(req, res, next);
  }
  
  app.use("/", readJsonBody({ limit: "100kb" }));
  
  app.get("/statistics", (req, res) => {
    console.log(`new request to get stats`);
    res.status(200).json({
      active_instances: functionsManager.activeCount,
      busy_instances: functionsManager.busyCount,
      total_invocations: functionsManager.invocationsCount
    })
  });

  app.listen(8001, () => {
    console.log(`handler listening on port 8001`);
  });
}

initHandler();
