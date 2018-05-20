class Topic {

    constructor(name) {
        this.name = name
        this.partitions = []
        this.subscribers = []
    }

    captureData(data) {
        this.paritions.push(data)
        this.subscribers.forEach((subscriber) => {
            subscriber.consume(this.partitions)
        })
    }

    addConsumer(consumer) {
        this.subscribers.push(consumer)
    }
}

class Producer {

    constructor(topicName) {
        this.topicName = topicName
    }

    publish(data) {

    }
}

class Consumer {

    constructor(broker, cb) {
        this.cb = cb
        this.offset = 0
        this.broker = broker
    }

    consume(partition) {
        if (this.cb) {
            var data = ""
            for (var i = this.offset; i < partitions.length; i++) {
                this.cb(data)
            }
            this.offset += partitions.length
        }
    }

    startConsuming(topicName) {
        const topic = this.broker.getTopic(topicName)
        if (topic) {
            topic.addConsumer(this)
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
