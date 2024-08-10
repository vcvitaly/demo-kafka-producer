package io.github.vcvitaly.demokafkaproducer.kafka;

import io.github.vcvitaly.demokafkaproducer.util.JsonUtil;
import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;

public abstract class BaseProducer<T> {

    private final KafkaTemplate<String, String> template;
    private final String topic;

    public BaseProducer(KafkaTemplate<String, String> template,
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
        final String json = JsonUtil.objToString(payload);
        template.send(topic, json).whenComplete((res, e) -> {
            final String id = getId(payload);
            if (e != null) {
                getLog().error("Error while producing a message with id [{}] to the topic [{}] - ", id, topic, e);
                throw new RuntimeException(e);
            } else {
                getLog().info("Send out a message with id [{}] to the topic [{}]", id, topic);
            }
        });
    }
}
