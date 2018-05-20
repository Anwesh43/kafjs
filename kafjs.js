class Topic {

    constructor(name) {
        this.name = name
        this.partitions = []
        this.subscribers = []
    }

    pushData(data) {
        this.partitions.push(data)
        this.subscribers.forEach((subscriber) => {
            subscriber.consume(this.partitions)
        })
    }

    addConsumer(consumer) {
        this.subscribers.push(consumer)
    }
}

class Producer {

    constructor(broker) {
        this.broker = broker
    }

    publish(data, topicName, cb) {
        const topic = this.broker.getTopic(topicName)
        topic.pushData(data)
    }
}

class Consumer {

    constructor(broker, cb) {
        this.cb = cb
        this.offset = 0
        this.broker = broker
    }

    consume(partitions) {
        if (this.cb) {
            var data = []
            var k = 0
            for (var i = this.offset; i < partitions.length; i++) {
                data.push(partitions[i])
                k++
            }
            this.cb(data)
            this.offset += k
        }
    }

    startConsuming(topicName) {
        if (this.offset == 0) {
            const topic = this.broker.getTopic(topicName)
            if (topic) {
                topic.addConsumer(this)
                if (topic.partitions.length > 0) {
                    this.consume(topic.partitions)
                }
            }
        }
        else {
            throw new Error("A consumer can only consume from one topic")
        }
    }
}

class Broker {

    constructor() {
        this.topics = []
    }

    getTopic(topicName) {
        for (let topic of this.topics) {
            if (topic.name == topicName) {
                return topic
            }
        }
    }

    createTopic(topicName) {
        const topic = new Topic(topicName)
        this.topics.push(topic)
    }
}

class Kaf {
    static createBroker() {
        if (!Kaf.broker) {
            Kaf.broker = new Broker()
        }
        return Kaf.broker
    }

    static createProducer() {
        if (Kaf.broker) {
            return new Producer(Kaf.broker)
        }
    }

    static createConsumer(topicName, cb) {
        if (Kaf.broker) {
            const consumer = new Consumer(Kaf.broker, cb)
            consumer.startConsuming(topicName)
            return consumer
        }
    }
}
