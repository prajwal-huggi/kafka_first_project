const {kafka}= require("./client");

const group= process.argv[2];

async function init(){
    const consumer= kafka.consumer({groupId:group});//very important to give the groupId to the consumer.js

    console.log("Connecting consumer");
    await consumer.connect();
    console.log("Connected consumer");
    
    // consumer will subscribe the topic
    await consumer.subscribe({topic:'rider-updated', fromBeginning: true});//fromBeginning means I want message from the beginning

    await consumer.run({
        eachMessage: async({topic, partition, message})=>{
            console.log(`GROUP:${group}: [${topic}]: PART:${partition}: `, message.value.toString())
        }
    })
}

init();