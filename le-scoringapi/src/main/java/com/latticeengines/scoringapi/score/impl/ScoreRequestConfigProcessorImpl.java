package com.latticeengines.scoringapi.score.impl;

import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.score.ScoreRequestConfigProcessor;

@Component("scoreRequestConfigProcessor")
public class ScoreRequestConfigProcessorImpl implements ScoreRequestConfigProcessor {

    private static final int INITIAL_CACHE_SIZE = 100;
    private static final int MAX_CACHE_SIZE = 10000;

    private Logger log = LoggerFactory.getLogger(ScoreRequestConfigProcessorImpl.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private Cache<String, ScoringRequestConfigContext> scoringRequestConfigCache;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        scoringRequestConfigCache = Caffeine.newBuilder().initialCapacity(INITIAL_CACHE_SIZE)
                .maximumSize(MAX_CACHE_SIZE).expireAfterWrite(6, TimeUnit.HOURS).build();
    }

    @Override
    public ScoringRequestConfigContext getScoreRequestConfigContext(String configId, String secretKey) {
        String cacheKey = StringUtils.join(new String[] { configId, secretKey }, "-");
        ScoringRequestConfigContext srcContext = scoringRequestConfigCache.getIfPresent(cacheKey);
        if (srcContext != null) {
            return srcContext;
        }
        try {
            srcContext = internalResourceRestApiProxy.retrieveScoringRequestConfigContext(configId);
        } catch (LedpException le) {
            if (LedpCode.LEDP_18194.equals(le.getCode())) {
                throw new ScoringApiException(LedpCode.LEDP_18194, new String[] { configId });
            }
            throw le;
        }
        if (!secretKey.equals(srcContext.getSecretKey())) {
            throw new ScoringApiException(LedpCode.LEDP_18200);
        }
        if (log.isDebugEnabled()) {
            log.debug("Caching ScoringRequest Configuration {}", configId);
        }
        scoringRequestConfigCache.put(cacheKey, srcContext);
        return srcContext;
    }

    @Override
    public ScoreRequest preProcessScoreRequestConfig(ScoreRequest scoreRequest,
            ScoringRequestConfigContext srcContext) {

        scoreRequest.setRecordId(String.valueOf(scoreRequest.getRecord().get("Id")));

        scoreRequest.setModelId(srcContext.getModelUuid());
        scoreRequest.setSource(srcContext.getExternalSystem().name());
        scoreRequest.setPerformEnrichment(false);
        if (StringUtils.isBlank(scoreRequest.getRule())) {
            scoreRequest.setRule("Not set");
        }
        return scoreRequest;
    }

}
