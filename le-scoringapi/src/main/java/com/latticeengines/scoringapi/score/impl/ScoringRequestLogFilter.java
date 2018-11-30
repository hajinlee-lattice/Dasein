package com.latticeengines.scoringapi.score.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;

import reactor.core.publisher.Mono;

@Component("scoringRequestLogFilter")
public class ScoringRequestLogFilter implements WebFilter {

    private static final Logger log = LoggerFactory.getLogger(ScoringRequestLogFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange serverWebExchange, WebFilterChain webFilterChain) {
        log.info("Before api");
        return webFilterChain.filter(serverWebExchange).doAfterTerminate(() -> log.info("After api"));
    }
}
