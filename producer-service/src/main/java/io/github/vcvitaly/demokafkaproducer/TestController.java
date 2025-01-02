package io.github.vcvitaly.demokafkaproducer;

import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.Executors;

@RestController
@RequiredArgsConstructor
@Slf4j
public class TestController {

    private final TestProducer testProducer;

    @PostMapping("/run")
    @ResponseStatus(HttpStatus.OK)
    public void runBatches() {
        /*CompletableFuture.runAsync(() -> {
            try {
                testProducer.produce();
            } catch (Exception e) {
                log.error("Error: ", e);
            }
        });*/
        try {
            testProducer.produce();
        } catch (Exception e) {
            log.error("Error: ", e);
        }
    }
}
