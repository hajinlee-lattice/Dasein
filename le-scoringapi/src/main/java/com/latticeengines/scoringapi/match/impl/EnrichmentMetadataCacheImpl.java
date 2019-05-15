package com.latticeengines.scoringapi.match.impl;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.match.EnrichmentMetadataCache;

@Component
public class EnrichmentMetadataCacheImpl implements EnrichmentMetadataCache {
    private static final Logger log = LoggerFactory.getLogger(EnrichmentMetadataCacheImpl.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Value("${scoringapi.enrichment.cache.size:100}")
    private int maxEnrichmentCacheSize;

    @Value("${scoringapi.enrichment.cache.expiration.time:5}")
    private int enrichmentCacheExpirationTime;

    @Value("${scoringapi.enrichment.all.fixedDelay.seconds:2}")
    private int fixedDelaySeconds;

    @Value("${scoringapi.enrichment.all.fixedRate.seconds:600}")
    private int fixedRateSeconds;

    private ReadWriteLock readWriteLock;

    private LoadingCache<CustomerSpace, List<LeadEnrichmentAttribute>> leadEnrichmentAttributeCache;

    private volatile List<LeadEnrichmentAttribute> allEnrichmentAttributes;

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        leadEnrichmentAttributeCache = //
                CacheBuilder.newBuilder()//
                        .maximumSize(maxEnrichmentCacheSize)//
                        .expireAfterWrite(enrichmentCacheExpirationTime, TimeUnit.MINUTES)//
                        .build(new CacheLoader<CustomerSpace, List<LeadEnrichmentAttribute>>() {
                            public List<LeadEnrichmentAttribute> load(CustomerSpace customerSpace) throws Exception {
                                return internalResourceRestApiProxy.getLeadEnrichmentAttributes(customerSpace, null,
                                        null, true, true);
                            }
                        });

        readWriteLock = new ReentrantReadWriteLock();

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(//
                createRefreshAllEnrichmentRunnable(), fixedDelaySeconds, //
                fixedRateSeconds, TimeUnit.SECONDS);
    }

    @Override
    public List<LeadEnrichmentAttribute> getEnrichmentAttributesMetadata(CustomerSpace space) {
        try {
            return leadEnrichmentAttributeCache.get(space);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, e, new String[] { e.getMessage() });
        }
    }

    @Override
    public List<LeadEnrichmentAttribute> getAllEnrichmentAttributesMetadata() {
        loadAllEnrichmentMetadataFirstTimeIfNeeded();
        readWriteLock.readLock().lock();
        try {
            return allEnrichmentAttributes;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private boolean loadAllEnrichmentMetadataFirstTimeIfNeeded() {
        boolean loadedDuringCall = false;
        if (allEnrichmentAttributes == null) {
            readWriteLock.writeLock().lock();
            try {
                if (allEnrichmentAttributes == null) {
                    allEnrichmentAttributes = loadAllEnrichmentAttributesMetadata();
                    if (allEnrichmentAttributes != null) {
                        loadedDuringCall = true;
                    }
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
        return loadedDuringCall;
    }

    private List<LeadEnrichmentAttribute> loadAllEnrichmentAttributesMetadata() {
        log.info("Start loading all enrichment attribute metadata");
        List<LeadEnrichmentAttribute> list = internalResourceRestApiProxy.getAllLeadEnrichmentAttributes();
        log.info("Completed loading all enrichment attribute metadata");
        return list;
    }

    private Runnable createRefreshAllEnrichmentRunnable() {
        return () -> {
            boolean loadedDuringCall = loadAllEnrichmentMetadataFirstTimeIfNeeded();

            if (!loadedDuringCall && allEnrichmentAttributes != null) {
                List<LeadEnrichmentAttribute> tempAllEnrichmentAttributes = loadAllEnrichmentAttributesMetadata();
                allEnrichmentAttributes = tempAllEnrichmentAttributes;

            }
        };
    }
}
