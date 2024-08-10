package io.github.vcvitaly.demokafkaproducer.kafka;

import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;

public abstract class BaseProducer<T> {

    private final KafkaTemplate<String, T> template;
    private final String topic;

    public BaseProducer(KafkaTemplate<String, T> template,
                        String topic) {
        this.template = template;
        this.topic = topic;
    }

    public void produce(T payload) {
        produceInternal(payload);
    }

    protected abstract Logger getLog();
    
    protected abstract String getId(T payload);

    private void produceInternal(T payload) {
        template.send(topic, payload).whenComplete((res, e) -> {
            final String id = getId(payload);
            if (e != null) {
                getLog().error("Error while producing a message with id [{}] to the topic [{}] - ", id, topic, e);
            } else {
                getLog().info("Send out a message with id [{}] to the topic [{}]", id, topic);
            }
        });
    }
}
