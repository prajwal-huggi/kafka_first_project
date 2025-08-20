const kafka = require("./client");

async function run() {
  const admin = kafka.admin();

  console.log("Connecting the admin");
  await admin.connect();
  console.log("Connected admin");

  await admin.createTopics({
    topics: [{ topic: "rider-updated", numPartitions: 2 }],
  });

  console.log("Topic created successfully!");

  console.log("Disconnecting the kafka");
  await admin.disconnect();
}

run().catch(console.error);
