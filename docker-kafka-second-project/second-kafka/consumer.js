const kafka = require("./client");

const group = process.argv[2];

async function run() {
  const consumer = kafka.consumer({ groupId: group });

  console.log("Connecting consumer");
  await consumer.connect();
  console.log("Connected consumer");

  await consumer.subscribe({ topic: "rider-topic", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        `GROUP:${group}: [${topic}]: PART:${partition}: `,
        message.value.toString()
      );
    },
  });
}

run().catch(console.error);
