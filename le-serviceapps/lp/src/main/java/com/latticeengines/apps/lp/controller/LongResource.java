package com.latticeengines.apps.lp.controller;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Loggers;

@RestController
@RequestMapping("/long")
public class LongResource {

    private AtomicLong counter = new AtomicLong();

    @GetMapping(value = "/mono")
    @ResponseBody
    public Mono<Long> getLong() {
        return Mono.just(counter.getAndIncrement());
    }

    @GetMapping(value = "/slow")
    @ResponseBody
    public Mono<Long> getLongSlow() {
        return Mono.delay(Duration.ofMillis(10000)).log(Loggers.getLogger(getClass()));
    }

    @GetMapping(value = "/slow2")
    @ResponseBody
    public Long getLongSlowBlocking() {
        return Mono.delay(Duration.ofMillis(10000)).log(Loggers.getLogger(getClass())).block();
    }

    @GetMapping(value = "/slow3", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
    @ResponseBody
    public Flux<Long> getLongSlowFlux() {
        Mono<Long> mono = Mono.delay(Duration.ofMillis(10000)).log(Loggers.getLogger(getClass()));
        return mono.flux();
    }

    @GetMapping(value = "/flux")
    @ResponseBody
    public Flux<Long> getLongs() {
        return Flux.range(1, 10).map(k -> counter.getAndIncrement()).log(Loggers.getLogger(getClass()));
    }

    @GetMapping(value = "/stream", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
    @ResponseBody
    public Flux<Long> getLongStream() {
        return Flux.interval(Duration.ofMillis(2000)).log(Loggers.getLogger(getClass()));
    }

    @GetMapping(value = "/stream2", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
    @ResponseBody
    public Flux<Long> getLongStream2() {
        return Flux.interval(Duration.ofMillis(10000)).log(Loggers.getLogger(getClass()));
    }

    @PutMapping(value = "/reset")
    @ResponseBody
    public void resetCounter() {
        counter.set(0);
    }

}
