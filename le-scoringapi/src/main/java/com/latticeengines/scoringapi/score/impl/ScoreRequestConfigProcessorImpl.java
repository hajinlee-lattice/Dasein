package com.latticeengines.scoringapi.score.impl;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.score.ScoreRequestConfigProcessor;

@Component("scoreRequestConfigProcessor")
public class ScoreRequestConfigProcessorImpl implements ScoreRequestConfigProcessor {

    @Value("${common.pls.url}")
    private String internalResourceHostPort;
    
    private InternalResourceRestApiProxy internalResourceRestApiProxy;
    
    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public ScoringRequestConfigContext getScoreRequestConfigContext(String configId, String secretKey) {
        ScoringRequestConfigContext srcContext;
        try {
            srcContext = internalResourceRestApiProxy
                    .retrieveScoringRequestConfigContext(configId);
        } catch (LedpException le) {
            if (LedpCode.LEDP_18194.equals(le.getCode())) {
                throw new ScoringApiException(LedpCode.LEDP_18194, new String[] {configId}) ;
            }
            throw le;
        }
        if(! secretKey.equals(srcContext.getSecretKey())) {
            throw new ScoringApiException(LedpCode.LEDP_18200);
        }
        
        return srcContext;
    }
    
    @Override
    public ScoreRequest preProcessScoreRequestConfig(ScoreRequest scoreRequest, ScoringRequestConfigContext srcContext) {
        
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
