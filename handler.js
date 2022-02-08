const express = require("express");
const Queue = require("./queue");
const FunctionsManager = require("./functionsManager");

const queue = new Queue({
  connectionConfig: {
    host: process.env.QUEUE_HOST, port: process.env.QUEUE_PORT, user: "guest", pass: "guest"
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

async function wait(timeMs) {
  return new Promise((resolve) => setTimeout(resolve, timeMs));
}

async function initHandler() {
  // wait for queue to be ready
  await wait(1000 * 15);

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

  app.listen(process.env.APP_PORT, () => {
    console.log(`handler listening on port ${process.env.APP_PORT}`);
  });
}

initHandler();
