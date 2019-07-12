package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.ExportFieldMetadataDefaultsService;
import com.latticeengines.apps.cdl.service.ExportFieldMetadataService;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

@Component
public abstract class ExportFieldMetadataServiceBase implements ExportFieldMetadataService {

    @Inject
    private ExportFieldMetadataDefaultsService defaultExportFieldMetadataService;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    private static Map<CDLExternalSystemName, ExportFieldMetadataService> registry = new HashMap<>();

    protected ExportFieldMetadataServiceBase(CDLExternalSystemName systemName) {
        registry.put(systemName, this);
    }

    public static ExportFieldMetadataService getExportFieldMetadataService(CDLExternalSystemName systemName) {
        if (!registry.containsKey(systemName)) {
            throw new LedpException(LedpCode.LEDP_00002);
        }

        return registry.get(systemName);
    }

    @Override
    public List<ExportFieldMetadataDefaults> getStandardExportFields(CDLExternalSystemName systemName) {
        return defaultExportFieldMetadataService.getAttributes(systemName);
    }

    @Override
    public Flux<ColumnMetadata> getServingMetadataForEntity(String customerSpace, BusinessEntity entity) {
        Flux<ColumnMetadata> cms = Flux.empty();
        if (entity != null) {
            List<ColumnMetadata> cmList = servingStoreProxy
                    .getDecoratedMetadata(customerSpace, entity, Collections.singletonList(Predefined.Enrichment))
                    .collectList().block();
            if (CollectionUtils.isNotEmpty(cmList)) {
                cms = Flux.fromIterable(cmList).filter(cm -> !AttrState.Inactive.equals(cm.getAttrState()));
            }
        }
        return cms;
    }

    public Flux<ColumnMetadata> getServingMetadata(String customerSpace, List<BusinessEntity> entities) {
        Flux<ColumnMetadata> cms = Flux.empty();
        if (entities != null && !entities.isEmpty()) {
            List<ColumnMetadata> cmList = new ArrayList<ColumnMetadata>();
            entities.forEach(entity -> {
                cmList.addAll(servingStoreProxy.getDecoratedMetadata(customerSpace, entity, Collections.singletonList(Predefined.Enrichment))
                .collectList().block());
            });
            if (CollectionUtils.isNotEmpty(cmList)) {
                cms = Flux.fromIterable(cmList).filter(cm -> !AttrState.Inactive.equals(cm.getAttrState()));
            }
        }
        return cms;
    }

    protected ColumnMetadata constructCampaignDerivedColumnMetadata(ExportFieldMetadataDefaults defaultExportField) {
        ColumnMetadata cm = new ColumnMetadata(defaultExportField.getAttrName(), defaultExportField.getJavaClass());
        cm.setDisplayName(defaultExportField.getDisplayName());
        cm.setIsCampaignDerivedField(true);
        cm.setEntity(defaultExportField.getEntity());
        return cm;
    }


}
