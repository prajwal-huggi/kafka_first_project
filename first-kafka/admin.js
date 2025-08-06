const { kafka } = require("./client");

async function init(){
    const admin= kafka.admin();
    console.log('Connecting admin');
    admin.connect();
    console.log("Successful connection of the admin");

    console.log("Creating topic [rider-updated]");
    await admin.createTopics({
        topics:[{
            topic:"rider-updated",
            numPartitions: 2
        }]
    });
    console.log("Created topic [rider-updated]");

    console.log("Disconnecting the kafka");
    await admin.disconnect();
}

init();