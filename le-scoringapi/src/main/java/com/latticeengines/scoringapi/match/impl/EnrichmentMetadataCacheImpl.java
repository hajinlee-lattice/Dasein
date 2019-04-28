package com.latticeengines.scoringapi.match.impl;

import java.util.List;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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

    @PostConstruct
    public void initialize() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public List<LeadEnrichmentAttribute> getEnrichmentAttributesMetadata(CustomerSpace space) {
        try {
            return internalResourceRestApiProxy.getLeadEnrichmentAttributes(space, null,
                    null, true, true);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, e, new String[] { e.getMessage() });
        }
    }

    @Override
    public List<LeadEnrichmentAttribute> getAllEnrichmentAttributesMetadata() {
        return internalResourceRestApiProxy.getAllLeadEnrichmentAttributes();
    }

}
