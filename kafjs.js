class Topic {

    constructor(name) {
        this.name = name
        this.partitions = []
        this.subscribers = []
    }

    captureData(data) {
        this.paritions.push(data)
        this.subscribers.consume(this)
    }
}

class Producer {

    constructor(topicName) {
        this.topicName = topicName
    }

    publish(data) {

    }
}
