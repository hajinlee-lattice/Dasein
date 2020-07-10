package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType.ENRICH;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

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

    @Override
    public List<PrimeAccount> fetch(Collection<String> dunsNumbers) {
        List<String> chunk = new ArrayList<>();
        List<PrimeAccount> results = new ArrayList<>();
        for (String dunsNumber: dunsNumbers) {
            chunk.add(dunsNumber);
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

    private List<PrimeAccount> fetchChunk(List<String> dunsNumbers) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("Retry attempt={} to fetch data block.", ctx.getRetryCount() + 1, ctx.getLastThrowable());
                SleepUtils.sleep(5000); // sleep 5 seconds to avoid blast D+ rate limit
            }
            try (PerformanceTimer timer = new PerformanceTimer("Fetch data block for " + //
                    dunsNumbers.size() + " DUNS numbers.")) {
                List<Callable<PrimeAccount>> callables = new ArrayList<>();
                dunsNumbers.forEach(dunsNumber -> //
                        callables.add(() -> {
                            try {
                                return new PrimeAccount(fetch(dunsNumber));
                            } catch (Exception e) {
                                log.error("Failed to fetch data block for DUNS {}", dunsNumber, e);
                                return null;
                            }
                        }));
                return ThreadPoolUtils.callInParallel(fetchers(), callables, //
                        1, TimeUnit.MINUTES, 250, TimeUnit.MILLISECONDS);
            }
        });
    }

    private Map<String, Object> fetch(String duns) {
        String resp = sendRequest(enrichUrl + "/duns/" + duns + "?blockIDs=companyinfo_L1_v1");
        return DirectPlusUtils.parseDataBlock(resp);
    }

    private String sendRequest(String url) {
        return _self.sendCacheableRequest(url);
    }

    @Cacheable(cacheNames = CacheName.Constants.DnBRealTimeLookup, key = "T(java.lang.String).format(\"%s\", #url)")
    public String sendCacheableRequest(String url) {
        String token = dnBAuthenticationService.requestToken(ENRICH, null);
        HttpEntity<String> entity = constructEntity(token);
        return client().get(entity, url);
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
            fetchers = ThreadPoolUtils.getFixedSizeThreadPool("data-block-fetcher", 8);
        }
    }

}
