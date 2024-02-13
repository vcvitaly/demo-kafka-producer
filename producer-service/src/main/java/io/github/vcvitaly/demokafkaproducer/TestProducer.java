package io.github.vcvitaly.demokafkaproducer;

import io.github.vcvitaly.producercommon.TestDto;
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

    public TestProducer(KafkaTemplate<String, TestDto> template,
                        @Value("${kafka.producer.topic}") String topic) {
        this.template = template;
        this.topic = topic;
        Executors.newSingleThreadScheduledExecutor().schedule(this::printStats, 10, TimeUnit.SECONDS);
        log.info("started");
    }

    private final LongAdder adder = new LongAdder();

    public void produce() {
        for (int i = 0; i < 1; i++) {
            template.send(topic, new TestDto(i, String.valueOf(i))).whenComplete((res, e) -> {
                if (e != null) {
                    log.error("Error while producing: ", e);
                } else {
                    adder.increment();
                }
            });
        }
    }

    private void printStats() {
        long l = adder.sumThenReset();
        if (l > 0) {
            log.info("Sent [%d] messages".formatted(l));
        }
    }
}
