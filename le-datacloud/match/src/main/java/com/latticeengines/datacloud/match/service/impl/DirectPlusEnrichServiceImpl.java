package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType.ENRICH;
import static org.springframework.http.HttpStatus.TOO_MANY_REQUESTS;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.xerial.snappy.Snappy;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.service.DirectPlusEnrichService;
import com.latticeengines.datacloud.match.util.DirectPlusUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;
import com.latticeengines.domain.exposed.datacloud.match.PrimeAccount;
import com.latticeengines.proxy.exposed.RestApiClient;

@Service
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class DirectPlusEnrichServiceImpl implements DirectPlusEnrichService {

    private static final Logger log = LoggerFactory.getLogger(DirectPlusEnrichServiceImpl.class);

    private static final String ATTR_KEY = "key";
    private static final String ATTR_TTL = "ttl";

    @Inject
    private DnBAuthenticationService dnBAuthenticationService;

    @Inject
    private ApplicationContext appCtx;

    @Value("${datacloud.dnb.direct.plus.data.block.chunk.size}")
    private int chunkSize;

    @Value("${datacloud.dnb.direct.plus.enrich.url}")
    private String enrichUrl;

    @Value("${datacloud.dnb.direct.plus.enrich.cache.dynamo.table}")
    private String cacheTableName;

    @Value("${datacloud.dnb.direct.plus.enrich.cache.ttl.min.days}")
    private int cacheTtlMinDays;

    @Value("${datacloud.dnb.direct.plus.enrich.cache.ttl.max.days}")
    private int cacheTtlMaxDays;

    @Inject
    private DynamoItemService dynamoItemService;

    private volatile RestApiClient apiClient;
    private volatile ExecutorService fetchers;
    private volatile ExecutorService cacheWriters;

    private AtomicLong stopUntil = new AtomicLong(0);
    private Random random = new Random(System.currentTimeMillis());

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
                        10, TimeUnit.MINUTES, 250, TimeUnit.MILLISECONDS);
            }
        });
    }

    private Map<String, Object> fetch(DirectPlusEnrichRequest request) {
        String duns = request.getDunsNumber();
        Map<String, List<PrimeColumn>> reqColumnsByBlock = new HashMap<>();
        // avoid alternating original List
        request.getReqColumnsByBlockId().forEach((k, v) -> reqColumnsByBlock.put(k, new ArrayList<>(v)));
        Map<String, Object> result = new HashMap<>();

        if (!request.isBypassDplusCache()) {
            Set<String> blockIds = consolidateFetchBlocks(reqColumnsByBlock.keySet());
            Map<String, String> respFromCache = readDplusCache(duns, blockIds);
            AtomicBoolean processedBaseInfo = new AtomicBoolean(false);
            respFromCache.forEach((blockId, blockResp) -> {
                List<PrimeColumn> reqColumnsInBlock = reqColumnsByBlock.get(blockId);
                if (!processedBaseInfo.get() && reqColumnsByBlock.containsKey("baseinfo_L1_v1")) {
                    reqColumnsInBlock.addAll(reqColumnsByBlock.get("baseinfo_L1_v1"));
                }
                Map<String, Object> blockResult = DirectPlusUtils.parseDataBlock(blockResp, reqColumnsInBlock);
                result.putAll(blockResult);
                reqColumnsByBlock.remove(blockId);
                if (!processedBaseInfo.get()) {
                    reqColumnsByBlock.remove("baseinfo_L1_v1");
                    processedBaseInfo.set(true);
                }
            });
        }

        if (!reqColumnsByBlock.isEmpty()) {
            Set<String> blockIds = consolidateFetchBlocks(reqColumnsByBlock.keySet());
            String url = enrichUrl + "/duns/" + duns + "?blockIDs=" + StringUtils.join(blockIds, ",");
            String resp = sendRequest(url);
            cacheDplusResponse(resp, request.getDunsNumber());
            List<PrimeColumn> reqColumns = new ArrayList<>();
            reqColumnsByBlock.values().forEach(reqColumns::addAll);
            Map<String, Object> fetchResult = DirectPlusUtils.parseDataBlock(resp, reqColumns);
            result.putAll(fetchResult);
        }

        return result;
    }

    private Set<String> consolidateFetchBlocks(Collection<String> blockIds) {
        if (CollectionUtils.isNotEmpty(blockIds)) {
            Set<String> toReturn = blockIds.stream().filter(block -> !block.startsWith("baseinfo"))
                    .collect(Collectors.toSet());
            if (!toReturn.isEmpty()) {
                return toReturn;
            }
        }
        return Collections.singleton("companyinfo_L1_v1");
    }

    public String sendRequest(String url) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(10);
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

    private void cacheDplusResponse(String resp, String duns) {
        ExecutorService writers = catchWriters();
        writers.submit(() -> {
            byte[] bytes;
            try {
                bytes = Snappy.compress(resp);
            } catch (IOException e) {
                log.error("Failed to compress D+ response: {}", resp, e);
                return;
            }

            Set<String> blockIds = DirectPlusUtils.parseCacheableBlockIds(resp);
            PrimaryKey primaryKey = new PrimaryKey(ATTR_KEY, duns);
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            Item item = retry.execute(ctx -> dynamoItemService.getItem(cacheTableName, primaryKey));
            if (item == null) {
                int ttlDays = cacheTtlMinDays + random.nextInt(Math.abs(cacheTtlMaxDays - cacheTtlMinDays));
                long expiredAt = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(ttlDays);
                item = new Item().withPrimaryKey(primaryKey).withNumber(ATTR_TTL, expiredAt);
                log.info("Going to create cache for duns={}, blockIds={}", duns, //
                        StringUtils.join(blockIds, ","));
            } else {
                log.info("Going to update cache for duns={}, blockIds={}", duns, //
                        StringUtils.join(blockIds, ","));
            }
            for (String blockId: blockIds) {
                item = item.withBinary(blockId, bytes);
            }
            dynamoItemService.putItem(cacheTableName, item);
        });
    }

    private Map<String, String> readDplusCache(String duns, Collection<String> blockIds) {
        Map<String, String> result = new HashMap<>();
        if (CollectionUtils.isNotEmpty(blockIds)) {
            PrimaryKey primaryKey = new PrimaryKey(ATTR_KEY, duns);
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            Item item = retry.execute(ctx -> dynamoItemService.getItem(cacheTableName, primaryKey));
            if (item != null) {
                for (String blockId: blockIds) {
                    if (item.hasAttribute(blockId)) {
                        try {
                            String resp = new String(Snappy.uncompress(item.getBinary(blockId)), //
                                    Charset.defaultCharset());
                            result.put(blockId, resp);
                        } catch (IOException e) {
                            log.error("Failed to extract D+ response from cache with key={}, attr={}", //
                                    duns, blockId, e);
                        }
                    }
                }
            }
        }
        return result;
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

    private ExecutorService catchWriters() {
        if (cacheWriters == null) {
            initCacheWriters();
        }
        return cacheWriters;
    }

    private synchronized void initCacheWriters() {
        if (cacheWriters == null) {
            cacheWriters = ThreadPoolUtils.getFixedSizeThreadPool("data-block-cache-writer", 4);
        }
    }

}
