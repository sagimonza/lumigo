const WORK_TIME = 1000 * 5;

function wait(timeMs) {
  return new Promise((resolve) => setTimeout(resolve, timeMs));
}

async function run(msg) {
  await wait(WORK_TIME);
  return msg;
}

process.on("message", async (msg) => {
  const result = await run(msg.message);
  process.send({ status: "completed", result });
});
