package com.latticeengines.pls.entitymanager.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoScoringMatchField;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.ScoringRequestConfigDao;
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;
import com.latticeengines.pls.entitymanager.MarketoScoringMatchFieldEntityMgr;
import com.latticeengines.pls.entitymanager.ScoringRequestConfigEntityManager;
import com.latticeengines.pls.repository.reader.ScoringRequestConfigReaderRepository;

@Component("scoringRequestConfigEntityMgr")
public class ScoringRequestConfigEntityManagerImpl extends BaseEntityMgrRepositoryImpl<ScoringRequestConfig, Long>  implements ScoringRequestConfigEntityManager {
    private static Logger LOG = Logger.getLogger(ScoringRequestConfigEntityManagerImpl.class);
    
    private static final String SCORING_REQUEST_CONFIG_ID_PREFIX = "src";
    private static final String SCORING_REQUEST_CONFIG_ID_FORMAT = "%s__%s";
    
    @Inject
    private MarketoCredentialEntityMgr marketoCredentialEntityMgr;
    
    @Inject
    private ScoringRequestConfigDao scoringRequestConfigDao;
    
    @Inject
    private ScoringRequestConfigReaderRepository scoringRequestReadRepository;
    
    @Inject
    private MarketoScoringMatchFieldEntityMgr marketoScoringMatchFieldEntityMgr;
    
    @Inject
    private TenantEntityMgr tenantEntityMgr;
    
    @Override
    public BaseJpaRepository<ScoringRequestConfig, Long> getRepository() {
        return scoringRequestReadRepository;
    }

    @Override
    public BaseDao<ScoringRequestConfig> getDao() {
        return scoringRequestConfigDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(ScoringRequestConfig scoringRequestConfig) {
        MarketoCredential marketoCredential = marketoCredentialEntityMgr
                .findMarketoCredentialById(scoringRequestConfig.getMarketoCredential().getPid().toString());
        if (marketoCredential == null) {
            throw new LedpException(LedpCode.LEDP_18197, new String[] {scoringRequestConfig.getMarketoCredential().getPid().toString()});
        }
        scoringRequestConfig.setMarketoCredential(marketoCredential);
        if (scoringRequestReadRepository.findByMarketoCredentialPidAndModelUuid(marketoCredential.getPid(), scoringRequestConfig.getModelUuid()) != null) {
            throw new LedpException(LedpCode.LEDP_18192, new String[] { marketoCredential.getName() });
        }
        preprocessCreate(scoringRequestConfig);
        scoringRequestConfig.getMarketoScoringMatchFields()
                .forEach(matchField -> { 
                        matchField.setScoringRequestConfig(scoringRequestConfig);
                        matchField.setTenant(scoringRequestConfig.getTenant());
                    });
        super.create(scoringRequestConfig);
    }

    private String generateRequestConfigId() {
        return String.format(SCORING_REQUEST_CONFIG_ID_FORMAT, SCORING_REQUEST_CONFIG_ID_PREFIX, UUID.randomUUID().toString());
    }
    
    private void preprocessCreate(ScoringRequestConfig scoringRequestConfig) {
        Tenant tenant = MultiTenantContext.getTenant();
        tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        scoringRequestConfig.setTenant(tenant);
        scoringRequestConfig.setConfigId(generateRequestConfigId());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ScoringRequestConfigSummary> findAllByMarketoCredential(Long credentialPid) {
        return scoringRequestReadRepository.findByMarketoCredentialPid(credentialPid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ScoringRequestConfig findByModelUuid(Long credentialPid, String modelUuid) {
        return scoringRequestReadRepository.findByMarketoCredentialPidAndModelUuid(credentialPid, modelUuid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ScoringRequestConfig findByConfigId(Long credentialPid, String configId) {
        return scoringRequestReadRepository.findByMarketoCredentialPidAndConfigId(credentialPid, configId);
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void update(ScoringRequestConfig scoringRequestConfig) {

        ScoringRequestConfig existingConfig = this.findByField("configId", scoringRequestConfig.getConfigId());
        if (existingConfig == null) {
            LOG.warn(String.format("Could not retrieve ScoringRequestConfig: %s, for Tenant: %s", scoringRequestConfig, MultiTenantContext.getTenant()));
            throw new LedpException(LedpCode.LEDP_18194, new String[] {scoringRequestConfig.getConfigId()});
        }
        
        //Handle FieldMappings
        Map<String, MarketoScoringMatchField> updatedFieldMappings = scoringRequestConfig.getMarketoScoringMatchFields()
                .stream()
                .collect(Collectors.toMap(MarketoScoringMatchField::getModelFieldName, matchField -> matchField));
        List<MarketoScoringMatchField> removedMappings = new ArrayList<>();
        
        // Update the existing mappings and remove the fields if they are not available in new collection
        Iterator<MarketoScoringMatchField> matchFieldIter = existingConfig.getMarketoScoringMatchFields().iterator();
        while (matchFieldIter.hasNext()) {
            MarketoScoringMatchField existingMatchField = matchFieldIter.next();
            MarketoScoringMatchField updatedField = updatedFieldMappings.get(existingMatchField.getModelFieldName());
            if (updatedField != null) {
                existingMatchField.setMarketoFieldName(updatedField.getMarketoFieldName());
                updatedFieldMappings.remove(existingMatchField.getModelFieldName());
            } else {
                matchFieldIter.remove();
                removedMappings.add(existingMatchField);
            }
        }
        // Add any new mappings that are newly selected
        updatedFieldMappings.values().forEach(matchField -> existingConfig.addMarketoScoringMatchField(matchField));
        
        marketoScoringMatchFieldEntityMgr.deleteFields(removedMappings);
        super.update(existingConfig);
    }

    @Override
    public ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid) {
        return scoringRequestReadRepository.retrieveScoringRequestConfigContext(configUuid);
    }

}
