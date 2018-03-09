package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
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

    @Override
    public List<CDLExternalSystemMapping> getExternalSystemByType(String customerSpace, CDLExternalSystemType type) {
        List<CDLExternalSystemMapping> systems = Collections.emptyList();

        Set<String> ids = getExternalSystemIds(type);
        if (CollectionUtils.isNotEmpty(ids)) {
            ParallelFlux<ColumnMetadata> cms = servingStoreService.getFullyDecoratedMetadata(BusinessEntity.Account);
            systems = cms.flatMap(cm -> {
                if (ids.contains(cm.getAttrName())) {
                    String attrName = cm.getAttrName();
                    String displayName = cm.getDisplayName();
                    return Mono.just(new CDLExternalSystemMapping(attrName, CDLExternalSystemMapping.FIELD_TYPE_STRING, displayName));
                } else {
                    return Mono.empty();
                }
            }).sequential().collectList().block();
        }
        return systems;
    }

    private Set<String> getExternalSystemIds(CDLExternalSystemType type) {
        CDLExternalSystem externalSystem = cdlExternalSystemEntityMgr.findExternalSystem();
        Set<String> ids;
        switch (type) {
            case CRM:
                ids = new HashSet<>(externalSystem.getCRMIdList());
                break;
            case ERP:
                ids = new HashSet<>(externalSystem.getERPIdList());
                break;
            case MAP:
                ids = new HashSet<>(externalSystem.getMAPIdList());
                break;
            case OTHER:
                ids = new HashSet<>(externalSystem.getOtherIdList());
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type.name());
        }
        return ids;
    }

    @Override
    public Map<String, List<CDLExternalSystemMapping>> getExternalSystemMap(String customerSpace) {
        Map<String, List<CDLExternalSystemMapping>> systemMap = new HashMap<>();
        List<CDLExternalSystemMapping> crm = getExternalSystemByType(customerSpace, CDLExternalSystemType.CRM);
        if (!CollectionUtils.isEmpty(crm)) {
            systemMap.put(CDLExternalSystemType.CRM.name(), crm);
        }
        List<CDLExternalSystemMapping> erp = getExternalSystemByType(customerSpace, CDLExternalSystemType.ERP);
        if (!CollectionUtils.isEmpty(erp)) {
            systemMap.put(CDLExternalSystemType.ERP.name(), erp);
        }
        List<CDLExternalSystemMapping> map = getExternalSystemByType(customerSpace, CDLExternalSystemType.MAP);
        if (!CollectionUtils.isEmpty(map)) {
            systemMap.put(CDLExternalSystemType.MAP.name(), map);
        }
        List<CDLExternalSystemMapping> other = getExternalSystemByType(customerSpace, CDLExternalSystemType.OTHER);
        if (!CollectionUtils.isEmpty(other)) {
            systemMap.put(CDLExternalSystemType.OTHER.name(), other);
        }
        return systemMap;
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
