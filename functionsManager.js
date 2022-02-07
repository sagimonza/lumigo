const fs = require("fs");
const { fork } = require("child_process");

const DEFAULT_IDLE_TIMEOUT = 1000 * 10;
const DEFAULT_BUSY_TIMEOUT = 1000 * 60 * 10;
const OUTPUT_LOG_PATH = "./output.log";


// todo: handle termination of the current process - gracefully terminate workers and don't accept incoming requests
// todo: optimize data structure for workers - members should be removed faster
// todo: add restrictions to constructor options

class FunctionsManager {
  _busyWorkers = [];
  _availableWorkers = [];
  _invocationsCount = 0;

  constructor(options = {}) {
    fs.writeFileSync(OUTPUT_LOG_PATH, "");
    this._idleTimeout = options.idleTimeout || DEFAULT_IDLE_TIMEOUT;
    this._busyTimeout = options.busyTimeout || DEFAULT_BUSY_TIMEOUT;
  } 

  dispatchFunction(msg) {
    const worker = this._getAvailabeWorker();
    this._assignWorkerMessage(worker, msg);
    this._invocationsCount++;
  }

  get activeCount() {
    return this._busyWorkers.length + this._availableWorkers.length;
  }

  get busyCount() {
    return this._busyWorkers.length;
  }

  get invocationsCount() {
    return this._invocationsCount;
  }

  _getAvailabeWorker() {
    if (this._availableWorkers.length === 0)
      this._availableWorkers.push({
        process: fork("./function.js"),
        idleTimer: null,
        initialized: false
      });
    return this._availableWorkers.pop();
  }

  _assignWorkerMessage(worker, msg) {
    clearTimeout(worker.idleTimer);
    worker.idleTimer = null;
    this._busyWorkers.push(worker);
    if (!worker.initialized) {
      worker.process.on("message", (msg) => {
        if (msg.status === "completed") {
          this._onFunctionCompleted(worker, msg);
        }
      });
      worker.process.on("exit", (code) => {
        console.log(`worker exited with code: ${code}`);
        this._removeWorker(worker);
      });
      worker.process.on("error", (err) => console.log(`worker error: ${err}`));
      worker.initialized = true;
    }

    this._scheduleWorkerTermination(worker, this._busyTimeout);
    worker.process.send({ message: msg });
  }

  _onFunctionCompleted(worker, msg) {
    fs.appendFileSync(OUTPUT_LOG_PATH, `[${new Date().toISOString()}] ${msg.result}\n`);
    this._removeFromArray(worker, this._busyWorkers);
    this._availableWorkers.push(worker);
    this._scheduleWorkerTermination(worker, this._idleTimeout);
  } 

  _scheduleWorkerTermination(worker, timeout) {
    if (worker.idleTimer) clearTimeout(worker.idleTimer);
    worker.idleTimer = setTimeout(() => worker.process.kill(), timeout);
  }

  _removeWorker(worker) {
    const isRemoved = this._removeFromArray(worker, this._availableWorkers);
    if (!isRemoved) this._removeFromArray(worker, this._busyWorkers);
  }

  _removeFromArray(member, array) {
    let memberIndex = array.indexOf(member);
    if (memberIndex > -1) {
      array.splice(memberIndex, 1);
      return true;
    }
  
    return false;
  }
}

module.exports = FunctionsManager;
