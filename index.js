const { Kafka } = require('kafkajs');

class KafkaLibrary
{
    #kafka;
    #clientId;
    #brokers;
    #producer;
    #consumer;
    #groupId;
    #currentTopic;

    constructor(clientId='app', brokers=['127.0.0.1:9092'], groupId='test-group')
    {
        if(Array.isArray(brokers)) {
            this.#brokers = brokers;
        } else {
            return "brokers should be in array format"
        }
        this.#clientId = clientId;
        // this.#topics = topics;
        this.#groupId = groupId;
        // this.#payload = payload;
    }
    producerConnect() 
    {
        this.#kafka = new Kafka({
            clientId: this.#clientId,
            brokers: this.#brokers,
            requestTimeout: 25000,
            // retry: {
            //     initialRetryTime: 100,
            //     retries: 8
            // }
        });
        this.#producer = this.#kafka.producer();
        // this.#consumer = this.#kafka.consumer({ groupId: this.#groupId });
    }
    consumerConnect() 
    {
        this.#kafka = new Kafka({
            clientId: this.#clientId,
            brokers: this.#brokers,
            requestTimeout: 25000,
            // retry: {
            //     initialRetryTime: 100,
            //     retries: 8
            // }
        });
        this.#consumer = this.#kafka.consumer({ groupId: this.#groupId });
    }
    // static init() 
    // {
    //     console.log('hello')
    //     this.connect();
    // }
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
    async consume(topic)
    {
        this.consumerConnect();
        // const topic = this.#topics
        await this.#consumer.connect()
        await this.#consumer.subscribe({ topic, fromBeginning: true })
        await this.#consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log({
                value: message.value.toString(),
                })
            },
        });
    }
}

module.exports = new KafkaLibrary;