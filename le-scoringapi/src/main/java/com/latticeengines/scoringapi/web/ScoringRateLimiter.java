package com.latticeengines.scoringapi.web;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import com.google.common.util.concurrent.RateLimiter;

@Component
public class ScoringRateLimiter extends OncePerRequestFilter {

    private static final String SCORE_SUBPATH = "score/record";

    @Value("${scoringapi.ratelimit.max:60}")
    private int permitsPerSecond;

    @Value("${scoringapi.ratelimit.bulk.requests.max:20}")
    private int maxBulkRequests;

    @Value("${scoringapi.ratelimit.single.requests.max:60}")
    private int maxSingleRequests;

    private int bulkRequestCost;

    private static RateLimiter rateLimiter;

    @PostConstruct
    public void init() {
        bulkRequestCost = maxSingleRequests / maxBulkRequests;
        rateLimiter = RateLimiter.create(permitsPerSecond);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, //
            HttpServletResponse response, FilterChain filterChain) //
            throws ServletException, IOException {

        // check for post request only
        if (isPostRequest(request)) {
            String uri = request.getRequestURI();

            // check for scoring calls only
            if (isScoreCall(uri)) {

                // check if too many live scoring requests
                if (shouldRejectScoringUnderHeavyLoad(uri)) {
                    response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
                    return;
                }
            }
        }
        filterChain.doFilter(request, response);
    }

    private boolean isPostRequest(HttpServletRequest request) {
        return HttpMethod.POST.name().equalsIgnoreCase(request.getMethod());
    }

    private boolean isScoreCall(String uri) {
        return uri.toLowerCase().contains(SCORE_SUBPATH);
    }

    private boolean shouldRejectScoringUnderHeavyLoad(String uri) {
        boolean shouldReject = false;
        if (uri.contains("score/records")) {
            shouldReject = !rateLimiter.tryAcquire(bulkRequestCost);
        } else if (uri.contains("score/record")) {
            shouldReject = !rateLimiter.tryAcquire(1);
        }

        return shouldReject;
    }

}
