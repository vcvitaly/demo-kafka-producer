package io.github.vcvitaly.demokafkaproducer;

import io.github.vcvitaly.producercommon.TestDto;
import io.github.vcvitaly.producercommon.TestType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@Component
@Slf4j
public class TestProducer {

    private final KafkaTemplate<String, TestDto> template;
    private final String topic;
    private final LongAdder adderCreated = new LongAdder();
    private final LongAdder adderUpdated = new LongAdder();

    public TestProducer(KafkaTemplate<String, TestDto> template,
                        @Value("${kafka.producer.topic}") String topic) {
        this.template = template;
        this.topic = topic;
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(this::printStats, 0, 10, TimeUnit.SECONDS);
    }

    public void produce() {
        for (int i = 0; i < 10_000; i++) {
            produceTestDto(i, TestType.CREATE, adderCreated);
            produceTestDto(i, TestType.UPDATE, adderUpdated);
        }
    }

    private void produceTestDto(int i, TestType type, LongAdder adder) {
        template.send(topic, new TestDto(i, type, String.valueOf(i))).whenComplete((res, e) -> {
            if (e != null) {
                log.error("Error while producing: ", e);
            } else {
                adder.increment();
            }
        });
    }

    private void printStats() {
        long created = adderCreated.sumThenReset();
        long updated = adderUpdated.sumThenReset();
        if (created > 0 || updated > 0) {
            log.info("Sent [created=%d,updated=%d] messages".formatted(created, updated));
        }
    }
}
