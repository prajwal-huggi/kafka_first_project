const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer();

  console.log("Connecting producer");
  await producer.connect();
  console.log("Producer is connected");

  rl.setPrompt("Enter input: ");
  rl.prompt();

  rl.on("line", async function (line) {
    const [riderName, location] = line.split(" ");

    await producer.send({
      topic: "rider-updated", //means on which topic we have to send
      messages: [
        {
          partition: location.toString().toLocaleLowerCase() == "south" ? 1 : 0,
          key: "name",
          value: JSON.stringify({ name: riderName, loc: location }),
        },
      ],
    });
  }).on('close', async()=>{
    await producer.disconnect();
  });
}

init();
