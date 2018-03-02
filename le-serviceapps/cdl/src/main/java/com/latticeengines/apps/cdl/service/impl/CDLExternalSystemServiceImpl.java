package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.CDLExternalSystemEntityMgr;
import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.attribute.PrimaryField;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;

@Component("cdlExternalSystemService")
public class CDLExternalSystemServiceImpl implements CDLExternalSystemService {

    @Inject
    private CDLExternalSystemEntityMgr cdlExternalSystemEntityMgr;

    @Inject
    private ServingStoreService servingStoreService;

    @Override
    public List<CDLExternalSystem> getAllExternalSystem(String customerSpace) {
        return cdlExternalSystemEntityMgr.findAllExternalSystem();
    }

    @Override
    public CDLExternalSystem getExternalSystem(String customerSpace) {
        return cdlExternalSystemEntityMgr.findExternalSystem();
    }

    @Override
    public void createOrUpdateExternalSystem(String customerSpace, CDLExternalSystem cdlExternalSystem) {
        CDLExternalSystem existingSystem = cdlExternalSystemEntityMgr.findExternalSystem();
        if (existingSystem == null) {
            cdlExternalSystem.setTenant(MultiTenantContext.getTenant());
            cdlExternalSystemEntityMgr.create(cdlExternalSystem);
        } else {
            existingSystem.setCrmIds(cdlExternalSystem.getCrmIds());
            existingSystem.setErpIds(cdlExternalSystem.getErpIds());
            existingSystem.setMapIds(cdlExternalSystem.getMapIds());
            existingSystem.setOtherIds(cdlExternalSystem.getOtherIds());
            cdlExternalSystemEntityMgr.update(existingSystem);
        }
    }

    public List<PrimaryField> getCRMPrimaryFields() {
        List<PrimaryField> fields = Collections.emptyList();
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        CDLExternalSystem externalSystem = getExternalSystem(customerSpace);
        Set<String> crmIds = new HashSet<>(externalSystem.getCRMIdList());
        if (CollectionUtils.isNotEmpty(crmIds)) {
            ParallelFlux<ColumnMetadata> cms = servingStoreService.getFullyDecoratedMetadata(BusinessEntity.Account);
            fields = cms.flatMap(cm -> {
                if (crmIds.contains(cm.getAttrName())) {
                    String attrName = cm.getAttrName();
                    String displayName = cm.getDisplayName();
                    String externalSystemName = cm.getAttrName();
                    return Mono.just(new PrimaryField(attrName, PrimaryField.FIELD_TYPE_STRING, displayName, externalSystemName));
                } else {
                    return Mono.empty();
                }
            }).sequential().collectList().block();
        }
        return fields;
    }

}
