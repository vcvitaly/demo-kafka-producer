package io.github.vcvitaly.demokafkaproducer.controller;

import io.github.vcvitaly.demokafkaproducer.service.DownstreamService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Slf4j
public class TestController {

    private final DownstreamService downstreamService;

    @PostMapping("/run")
    @ResponseStatus(HttpStatus.OK)
    public void runBatches() {
        downstreamService.produce();
    }
}
