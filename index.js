const { Kafka } = require('kafkajs');

class KafkaLibrary
{
    #kafka;
    #clientId;
    #brokers;
    #producer;
    #consumer;
    #groupId;

    constructor(clientId='app', brokers=['127.0.0.1:9092'], groupId='test-group')
    {
        if(Array.isArray(brokers)) {
            this.#brokers = brokers;
        } else {
            return "brokers should be in array format"
        }
        this.#clientId = clientId;
        this.#groupId = groupId;
    }
    producerConnect() 
    {
        this.#kafka = new Kafka({
            clientId: this.#clientId,
            brokers: this.#brokers,
            requestTimeout: 25000,
           
        });
        this.#producer = this.#kafka.producer();
    }
    consumerConnect() 
    {
        this.#kafka = new Kafka({
            clientId: this.#clientId,
            brokers: this.#brokers,
            requestTimeout: 25000,
          
        });
        this.#consumer = this.#kafka.consumer({ groupId: this.#groupId });
    }
  
    async produce(topic, payload)
    {
        this.producerConnect();
        const messages = JSON.stringify(payload)
        await this.#producer.connect();
        await this.#producer.send({
            topic,
            messages: [
                { value: messages },
            ],
        })
        // await this.#producer.disconnect();
    }
    async getPayload(payload) {
        console.log(payload.toString())
        const payloadSend = JSON.stringify(payload.toString())
        return payloadSend
    }

    async consume(topic)
    {
        let _this = this;
        this.consumerConnect();
        await this.#consumer.connect()
        await this.#consumer.subscribe({ topic, fromBeginning: true })
        await this.#consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
               await _this.getPayload(message.value)
                console.log({
                value: message.value.toString(),
                })
            },
        });
    }
}

module.exports = new KafkaLibrary;