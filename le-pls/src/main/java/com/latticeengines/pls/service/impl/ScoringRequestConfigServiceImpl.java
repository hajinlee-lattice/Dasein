package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MarketoScoringMatchField;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary;
import com.latticeengines.pls.entitymanager.ScoringRequestConfigEntityManager;
import com.latticeengines.pls.service.ScoringRequestConfigService;

/**
 * @author jadusumalli
 * Serves the ScoringRequestConfiguration functionality for Marketo Integration
 *
 */
@Component
public class ScoringRequestConfigServiceImpl implements ScoringRequestConfigService {

    @Inject
    private ScoringRequestConfigEntityManager scoringRequestConfigEntityMgr;

    @Value("${pls.marketo.scoring.webhook.resource}")
    private String webhookResource;

    @Override
    public void createScoringRequestConfig(ScoringRequestConfig scoringRequestConfig) {
        validateAndProcessScoringRequestConfig(scoringRequestConfig, false);
        scoringRequestConfigEntityMgr.create(scoringRequestConfig);
    }

    private void validateAndProcessScoringRequestConfig(ScoringRequestConfig scoringRequestConfig, boolean updateRequest) {
        if(CollectionUtils.isEmpty(scoringRequestConfig.getMarketoScoringMatchFields())) {
            throw new LedpException(LedpCode.LEDP_18191, new String[] {"scoring field mappings"});
        }
        List<MarketoScoringMatchField> validMappings = scoringRequestConfig.getMarketoScoringMatchFields().stream()
                .filter(matchField -> StringUtils.isNotBlank(matchField.getModelFieldName())
                        && StringUtils.isNotBlank(matchField.getMarketoFieldName()))
                .collect(Collectors.toList());
        if(CollectionUtils.isEmpty(validMappings)) {
            throw new LedpException(LedpCode.LEDP_18191, new String[] {"field mappings. Field mappings cannot be blank."});
        }
        scoringRequestConfig.setMarketoScoringMatchFields(validMappings);
        scoringRequestConfig.setWebhookResource(webhookResource);
        if(StringUtils.isBlank(scoringRequestConfig.getModelUuid())) {
            throw new LedpException(LedpCode.LEDP_18191, new String[] {"Model association"});
        }
        if(scoringRequestConfig.getMarketoCredential() == null) {
            throw new LedpException(LedpCode.LEDP_18191, new String[] {"Marketo Profile association"});
        }

        // Validate update request fields
        if (updateRequest) {
            if(StringUtils.isBlank(scoringRequestConfig.getConfigId())) {
                throw new LedpException(LedpCode.LEDP_18193, new String[] {"Missing required Configuration ID"});
            }
        }

    }

    @Override
    public List<ScoringRequestConfigSummary> findAllByMarketoCredential(Long credentialPid) {
        return scoringRequestConfigEntityMgr.findAllByMarketoCredential(credentialPid);
    }

    @Override
    public ScoringRequestConfig findByModelUuid(Long credentialPid, String modelUuid) {
        ScoringRequestConfig scoringReqConfig = scoringRequestConfigEntityMgr.findByModelUuid(credentialPid, modelUuid);
        addUserInterfaceContext(scoringReqConfig);
        return scoringReqConfig;
    }

    @Override
    public ScoringRequestConfig findByConfigId(Long credentialPid, String configId) {
        ScoringRequestConfig scoringReqConfig = scoringRequestConfigEntityMgr.findByConfigId(credentialPid, configId);
        addUserInterfaceContext(scoringReqConfig);
        return scoringReqConfig;
    }

    private void addUserInterfaceContext(ScoringRequestConfig scoringReqConfig) {
        if (scoringReqConfig == null) {
            return;
        }
        scoringReqConfig.setWebhookResource(webhookResource);
    }

    @Override
    public void updateScoringRequestConfig(ScoringRequestConfig scoringRequestConfig) {
        validateAndProcessScoringRequestConfig(scoringRequestConfig, true);
        scoringRequestConfigEntityMgr.update(scoringRequestConfig);
    }

    @Override
    public ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid) {
        if (StringUtils.isBlank(configUuid)) {
            throw new LedpException(LedpCode.LEDP_18194, new String[] {configUuid});
        }
        return scoringRequestConfigEntityMgr.retrieveScoringRequestConfigContext(configUuid);
    }

}
