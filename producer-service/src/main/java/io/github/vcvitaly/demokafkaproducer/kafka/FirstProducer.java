package io.github.vcvitaly.demokafkaproducer.kafka;

import io.github.vcvitaly.producercommon.TestDto;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class FirstProducer extends BaseProducer<TestDto> {

    public FirstProducer(KafkaTemplate<String, TestDto> template,
                         @Value("${kafka.producer.topic1}") String topic) {
        super(template, topic);
    }

    @Override
    protected Logger getLog() {
        return log;
    }

    @Override
    protected String getId(TestDto payload) {
        return String.valueOf(payload.id());
    }
}
