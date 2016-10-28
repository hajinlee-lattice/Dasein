package com.latticeengines.scoringapi.match.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

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
    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Value("${scoringapi.enrichment.cache.size:100}")
    private int maxEnrichmentCacheSize;

    @Value("${scoringapi.enrichment.cache.expiration.time:5}")
    private int enrichmentCacheExpirationTime;

    private LoadingCache<CustomerSpace, List<LeadEnrichmentAttribute>> leadEnrichmentAttributeCache;

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
                                        null, true);
                            }
                        });
    }

    @Override
    public List<LeadEnrichmentAttribute> getEnrichmentAttributesMetadata(CustomerSpace space) {
        try {
            return leadEnrichmentAttributeCache.get(space);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, e, new String[] { e.getMessage() });
        }
    }
}
