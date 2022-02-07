const express = require("express");
const FunctionsManager = require("./functionsManager");

const functionsManager = new FunctionsManager();

const app = express();

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

// validate msg, handle errors
app.post("/messages", (req, res) => {
  console.log(`new request to dispatch function`);
  if (!req.body?.message) return res.status(400).json({ error: "missing message property on request body" });

  functionsManager.dispatchFunction(req.body.message);
  res.status(200).json({ data: "message dispatched" });
});

app.listen(8000, () => {
  console.log(`api listening on port 8000`);
});
