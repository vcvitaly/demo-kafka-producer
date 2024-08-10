package io.github.vcvitaly.demokafkaproducer.service;

import io.github.vcvitaly.demokafkaproducer.kafka.FirstProducer;
import io.github.vcvitaly.demokafkaproducer.kafka.SecondProducer;
import io.github.vcvitaly.producercommon.TestDto;
import io.github.vcvitaly.producercommon.TestType;
import io.github.vcvitaly.producercommon.TimestampedDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@RequiredArgsConstructor
@Service
public class DownstreamService {

    private final FirstProducer firstProducer;
    private final SecondProducer secondProducer;

    @Transactional
    public void produce() {
        final long currentTimeMillis = System.currentTimeMillis();
        firstProducer.produce(new TestDto((int) (currentTimeMillis / 1000), TestType.CREATE, String.valueOf(currentTimeMillis)));
        secondProducer.produce(new TimestampedDto("test_guid", currentTimeMillis, "Hello world!"));
    }
}
