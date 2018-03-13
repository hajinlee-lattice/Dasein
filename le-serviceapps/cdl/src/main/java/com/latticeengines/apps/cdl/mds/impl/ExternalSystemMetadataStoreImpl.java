package com.latticeengines.apps.cdl.mds.impl;

import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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


@Component
public class ExternalSystemMetadataStoreImpl implements ExternalSystemMetadataStore {

    private final CDLExternalSystemEntityMgr cdlExternalSystemEntityMgr;

    private final CDLNamespaceService cdlNamespaceService;

    @Inject
    public ExternalSystemMetadataStoreImpl(CDLExternalSystemEntityMgr cdlExternalSystemEntityMgr, CDLNamespaceService cdlNamespaceService) {
        this.cdlExternalSystemEntityMgr = cdlExternalSystemEntityMgr;
        this.cdlNamespaceService = cdlNamespaceService;
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace2<String, BusinessEntity> namespace) {
        Flux<ColumnMetadata> flux = Flux.empty();
        // only account has external system ids now
        if (BusinessEntity.Account.equals(namespace.getCoord2())) {
            cdlNamespaceService.setMultiTenantContext(namespace.getCoord1());
            CDLExternalSystem externalSystem = cdlExternalSystemEntityMgr.findExternalSystem();
            Set<String> ids = new HashSet<>();
            if (CollectionUtils.isNotEmpty(externalSystem.getCRMIdList())) {
                ids.addAll(externalSystem.getCRMIdList());
            }
            if (CollectionUtils.isNotEmpty(externalSystem.getMAPIdList())) {
                ids.addAll(externalSystem.getMAPIdList());
            }
            if (CollectionUtils.isNotEmpty(externalSystem.getERPIdList())) {
                ids.addAll(externalSystem.getERPIdList());
            }
            if (CollectionUtils.isNotEmpty(externalSystem.getOtherIdList())) {
                ids.addAll(externalSystem.getOtherIdList());
            }
            if (CollectionUtils.isNotEmpty(ids)) {
                flux = Flux.fromIterable(ids).map(id -> {
                    ColumnMetadata cm = new ColumnMetadata();
                    cm.setAttrName(id);
                    cm.enableGroup(ColumnSelection.Predefined.LookupId);
                    return cm;
                });
            }
        }
        return flux;
    }

}
