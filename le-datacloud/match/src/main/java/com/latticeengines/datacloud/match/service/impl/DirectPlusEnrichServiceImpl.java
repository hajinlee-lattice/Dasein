package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType.ENRICH;
import static org.springframework.http.HttpStatus.TOO_MANY_REQUESTS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.service.DirectPlusEnrichService;
import com.latticeengines.datacloud.match.util.DirectPlusUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.datacloud.match.PrimeAccount;
import com.latticeengines.proxy.exposed.RestApiClient;

@Service
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class DirectPlusEnrichServiceImpl implements DirectPlusEnrichService {

    private static final Logger log = LoggerFactory.getLogger(DirectPlusEnrichServiceImpl.class);

    @Inject
    private DnBAuthenticationService dnBAuthenticationService;

    @Inject
    private ApplicationContext appCtx;

    @Inject
    private DirectPlusEnrichServiceImpl _self;

    @Value("${datacloud.dnb.direct.plus.data.block.chunk.size}")
    private int chunkSize;

    @Value("${datacloud.dnb.direct.plus.enrich.url}")
    private String enrichUrl;

    private volatile RestApiClient apiClient;
    private volatile ExecutorService fetchers;

    private AtomicLong stopUntil = new AtomicLong(0);

    @Override
    public List<PrimeAccount> fetch(Collection<DirectPlusEnrichRequest> requests) {
        List<DirectPlusEnrichRequest> chunk = new ArrayList<>();
        List<PrimeAccount> results = new ArrayList<>();
        for (DirectPlusEnrichRequest request: requests) {
            chunk.add(request);
            if (chunk.size() >= chunkSize) {
                List<PrimeAccount> chunkResult = fetchChunk(chunk);
                results.addAll(chunkResult);
                chunk.clear();
            }
        }
        if (!chunk.isEmpty()) {
            List<PrimeAccount> chunkResult = fetchChunk(chunk);
            results.addAll(chunkResult);
            chunk.clear();
        }
        return results;
    }

    private List<PrimeAccount> fetchChunk(List<DirectPlusEnrichRequest> requests) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("Retry attempt={} to fetch data block.", ctx.getRetryCount() + 1, ctx.getLastThrowable());
                SleepUtils.sleep(5000); // sleep 5 seconds to avoid blast D+ rate limit
            }
            try (PerformanceTimer timer = new PerformanceTimer("Fetch data block for " + //
                    requests.size() + " DUNS numbers.")) {
                List<Callable<PrimeAccount>> callables = new ArrayList<>();
                requests.forEach(request -> //
                        callables.add(() -> {
                            try {
                                return new PrimeAccount(fetch(request));
                            } catch (Exception e) {
                                log.error("Failed to fetch data block for DUNS {}", request.getDunsNumber(), e);
                                return null;
                            }
                        }));
                return ThreadPoolUtils.callInParallel(fetchers(), callables, //
                        5, TimeUnit.MINUTES, 250, TimeUnit.MILLISECONDS);
            }
        });
    }

    private Map<String, Object> fetch(DirectPlusEnrichRequest request) {
        String url = enrichUrl + "/duns/" + request.getDunsNumber() + "?blockIDs=";
        url += StringUtils.join(request.getBlockIds(), ",");
        String resp = sendRequest(url);
        return DirectPlusUtils.parseDataBlock(resp, request.getReqColumns());
    }

    private String sendRequest(String url) {
        return _self.sendCacheableRequest(url);
    }

    @Cacheable(cacheNames = CacheName.Constants.DnBRealTimeLookup, key = "T(java.lang.String).format(\"%s\", #url)")
    public String sendCacheableRequest(String url) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            while (System.currentTimeMillis() < stopUntil.get()) {
                log.info("In peace period, wait 5 seconds.");
                SleepUtils.sleep(5000);
            }
            String token = dnBAuthenticationService.requestToken(ENRICH, null);
            HttpEntity<String> entity = constructEntity(token);
            try (PerformanceTimer timer = new PerformanceTimer("Fetching datablock from " + url)) {
                timer.setThreshold(10000);
                return client().get(entity, url);
            } catch (HttpClientErrorException e) {
                log.warn("Got http client error when calling {}", url, e);
                if (TOO_MANY_REQUESTS.equals(e.getStatusCode())) {
                    // this is the only exception we want to retry
                    // hold off 10 second
                    stopUntil.set(System.currentTimeMillis() + 10000);
                    log.info("TOO_MANY_REQUESTS, pause fetching for 10 second.");
                } else {
                    // not retry any more
                    ctx.setExhaustedOnly();
                }
                throw e;
            } catch (Exception e2) {
                // not retry any more
                ctx.setExhaustedOnly();
                throw e2;
            }
        });
    }

    private HttpEntity<String> constructEntity(String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        return new HttpEntity<>("", headers);
    }

    private RestApiClient client() {
        if (apiClient == null) {
            initApiClient();
        }
        return apiClient;
    }

    private synchronized void initApiClient() {
        if (apiClient == null) {
            apiClient = RestApiClient.newExternalClient(appCtx);
            apiClient.setErrorHandler(new GetDnBResponseErrorHandler());
            apiClient.setUseUri(true);
        }
    }

    private ExecutorService fetchers() {
        if (fetchers == null) {
            initFetchers();
        }
        return fetchers;
    }

    private synchronized void initFetchers() {
        if (fetchers == null) {
            fetchers = ThreadPoolUtils.getFixedSizeThreadPool("data-block-fetcher", 4);
        }
    }

}
