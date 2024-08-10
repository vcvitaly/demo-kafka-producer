package io.github.vcvitaly.demokafkaproducer.kafka;

import io.github.vcvitaly.producercommon.TimestampedDto;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SecondProducer extends BaseProducer<TimestampedDto> {

    public SecondProducer(KafkaTemplate<String, String> template,
                          @Value("${kafka.producer.topic2}") String topic) {
        super(template, topic);
    }

    @Override
    public void produce(TimestampedDto payload) {
        throw new IllegalStateException("Something went wrong");
    }

    @Override
    protected Logger getLog() {
        return log;
    }

    @Override
    protected String getId(TimestampedDto payload) {
        return payload.guid();
    }
}
