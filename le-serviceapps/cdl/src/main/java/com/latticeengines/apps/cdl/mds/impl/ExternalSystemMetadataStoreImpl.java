package com.latticeengines.apps.cdl.mds.impl;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.CDLExternalSystemEntityMgr;
import com.latticeengines.apps.cdl.mds.ExternalSystemMetadataStore;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Flux;

// External Lookup Ids for Account Category

@Component
public class ExternalSystemMetadataStoreImpl implements ExternalSystemMetadataStore {

    private final CDLExternalSystemEntityMgr cdlExternalSystemEntityMgr;

    private final CDLNamespaceService cdlNamespaceService;

    @Inject
    public ExternalSystemMetadataStoreImpl(CDLExternalSystemEntityMgr cdlExternalSystemEntityMgr,
            CDLNamespaceService cdlNamespaceService) {
        this.cdlExternalSystemEntityMgr = cdlExternalSystemEntityMgr;
        this.cdlNamespaceService = cdlNamespaceService;
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace2<String, BusinessEntity> namespace) {
        Flux<ColumnMetadata> flux = Flux.empty();
        // only account has external system ids now
        if (BusinessEntity.Account.equals(namespace.getCoord2())) {
            cdlNamespaceService.setMultiTenantContext(namespace.getCoord1());
            CDLExternalSystem externalSystem = cdlExternalSystemEntityMgr.findExternalSystem(BusinessEntity.Account);

            if (externalSystem != null) {
                if (CollectionUtils.isNotEmpty(externalSystem.getCRMIdList())) {
                    flux = flux.concatWith(Flux.fromIterable(externalSystem.getCRMIdList())
                            .map(id -> toColumnMetadata(id, "CRM", externalSystem.getDisplayNameById(id))));
                }
                if (CollectionUtils.isNotEmpty(externalSystem.getMAPIdList())) {
                    flux = flux.concatWith(Flux.fromIterable(externalSystem.getMAPIdList())
                            .map(id -> toColumnMetadata(id, "MAP", externalSystem.getDisplayNameById(id))));
                }
                if (CollectionUtils.isNotEmpty(externalSystem.getERPIdList())) {
                    flux = flux.concatWith(Flux.fromIterable(externalSystem.getERPIdList())
                            .map(id -> toColumnMetadata(id, "ERP", externalSystem.getDisplayNameById(id))));
                }
                if (CollectionUtils.isNotEmpty(externalSystem.getOtherIdList())) {
                    flux = flux.concatWith(Flux.fromIterable(externalSystem.getOtherIdList())
                            .map(id -> toColumnMetadata(id, null, externalSystem.getDisplayNameById(id))));
                }
            }
        }
        return flux;
    }

    private ColumnMetadata toColumnMetadata(String attrName, String type, String displayName) {
        ColumnMetadata cm = new ColumnMetadata();
        cm.setAttrName(attrName);
        cm.enableGroup(ColumnSelection.Predefined.LookupId);
        cm.setSubcategory("Account IDs");
        if (StringUtils.isNotBlank(type)) {
            cm.setSecondaryDisplayName(type);
        }
        if (StringUtils.isNotBlank(displayName)) {
            cm.setDisplayName(displayName);
        }
        cm.setCanModel(false);
        return cm;
    }

}
