package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.ExportFieldMetadataDefaultsService;
import com.latticeengines.apps.cdl.service.ExportFieldMetadataService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

import reactor.core.publisher.Flux;

@Component
public abstract class ExportFieldMetadataServiceBase implements ExportFieldMetadataService {

    private static Logger log = LoggerFactory.getLogger(ExportFieldMetadataServiceBase.class);

    @Inject
    private ExportFieldMetadataDefaultsService defaultExportFieldMetadataService;

    @Inject
    private ServingStoreService servingStoreService;

    private static Map<CDLExternalSystemName, ExportFieldMetadataService> registry = new HashMap<>();

    protected ExportFieldMetadataServiceBase() {
    }

    protected ExportFieldMetadataServiceBase(CDLExternalSystemName systemName) {
        registry.put(systemName, this);
    }

    protected ExportFieldMetadataServiceBase(List<CDLExternalSystemName> systemNames) {
        systemNames.forEach(systemName -> registry.put(systemName, this));
    }

    public static ExportFieldMetadataService getExportFieldMetadataService(CDLExternalSystemName systemName) {
        if (!registry.containsKey(systemName)) {
            throw new LedpException(LedpCode.LEDP_40068, new String[] { systemName.toString() });
        }

        return registry.get(systemName);
    }

    protected Map<String, ExportFieldMetadataDefaults> getDefaultExportFieldsMap(CDLExternalSystemName systemName) {
        return defaultExportFieldMetadataService.getExportEnabledAttributes(systemName).stream()
                .collect(Collectors.toMap(ExportFieldMetadataDefaults::getAttrName, Function.identity()));
    }

    protected List<ColumnMetadata> enrichExportFieldMappings(CDLExternalSystemName systemName,
            List<String> fieldNames, Map<String, ColumnMetadata> accountAttributesMap,
            Map<String, ColumnMetadata> contactAttributesMap) {

        List<ColumnMetadata> exportColumnMetadataList = new ArrayList<ColumnMetadata>();
        Map<String, ExportFieldMetadataDefaults> defaultFieldsMetadataMap = getDefaultExportFieldsMap(systemName);

        fieldNames.forEach(fieldName -> {
            ExportFieldMetadataDefaults defaultField = defaultFieldsMetadataMap.get(fieldName);
            if (defaultField == null) {
                throw new LedpException(LedpCode.LEDP_40069, new String[] { fieldName, systemName.name() });
            }
            String attrName = defaultField.getAttrName();
            ColumnMetadata cm;

            if (defaultField.getStandardField() && accountAttributesMap.containsKey(attrName)) {
                cm = accountAttributesMap.get(attrName);
                cm.setDisplayName(defaultField.getDisplayName());
                accountAttributesMap.remove(attrName);
            } else if (defaultField.getStandardField() && contactAttributesMap.containsKey(attrName)) {
                cm = contactAttributesMap.get(attrName);
                cm.setDisplayName(defaultField.getDisplayName());
                contactAttributesMap.remove(attrName);
            } else {
                cm = constructCampaignDerivedColumnMetadata(defaultField);
            }

            exportColumnMetadataList.add(cm);
        });

        return exportColumnMetadataList;
    }

    protected List<ColumnMetadata> enrichDefaultFieldsMetadata(String customerSpace, CDLExternalSystemName systemName) {

        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Account));

        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Contact));

        return enrichDefaultFieldsMetadata(systemName, accountAttributesMap, contactAttributesMap);
    }

    protected List<ColumnMetadata> enrichDefaultFieldsMetadata(CDLExternalSystemName systemName,
            Map<String, ColumnMetadata> accountAttributesMap, Map<String, ColumnMetadata> contactAttributesMap) {

        List<ColumnMetadata> exportColumnMetadataList = new ArrayList<ColumnMetadata>();
        Map<String, ExportFieldMetadataDefaults> defaultFieldsMetadataMap = getDefaultExportFieldsMap(systemName);

        defaultFieldsMetadataMap.values().forEach(defaultField -> {
            String attrName = defaultField.getAttrName();
            ColumnMetadata cm;

            if (defaultField.getStandardField() && accountAttributesMap.containsKey(attrName)) {
                cm = accountAttributesMap.get(attrName);
                cm.setDisplayName(defaultField.getDisplayName());
                accountAttributesMap.remove(attrName);
            } else if (defaultField.getStandardField() && contactAttributesMap.containsKey(attrName)) {
                cm = contactAttributesMap.get(attrName);
                cm.setDisplayName(defaultField.getDisplayName());
                contactAttributesMap.remove(attrName);
            } else {
                cm = constructCampaignDerivedColumnMetadata(defaultField);
            }

            exportColumnMetadataList.add(cm);
        });

        return exportColumnMetadataList;
    }

    protected Flux<ColumnMetadata> getServingMetadata(String customerSpace, List<BusinessEntity> entities) {
        Flux<ColumnMetadata> cms = Flux.empty();
        if (entities != null && !entities.isEmpty()) {
            List<ColumnMetadata> cmList = new ArrayList<ColumnMetadata>();
            entities.forEach(entity -> {
                cmList.addAll(servingStoreService.getDecoratedMetadata(customerSpace, entity,
                        null, Collections.singletonList(Predefined.Enrichment))
                        .collectList().block());
            });
            if (CollectionUtils.isNotEmpty(cmList)) {
                cms = Flux.fromIterable(cmList).filter(cm -> !AttrState.Inactive.equals(cm.getAttrState()));
            }
        }
        return cms;
    }

    protected Map<String, ColumnMetadata> getServingMetadataMap(String customerSpace, List<BusinessEntity> entities) {
        return getServingMetadata(customerSpace, entities)
                .collect(HashMap<String, ColumnMetadata>::new, (returnMap, cm) -> returnMap.put(cm.getAttrName(), cm))
                .block();
    }

    protected ColumnMetadata constructCampaignDerivedColumnMetadata(ExportFieldMetadataDefaults defaultExportField) {
        ColumnMetadata cm = new ColumnMetadata(defaultExportField.getAttrName(), defaultExportField.getJavaClass());
        cm.setDisplayName(defaultExportField.getDisplayName());
        cm.setIsCampaignDerivedField(true);
        cm.setEntity(defaultExportField.getEntity());
        return cm;
    }

}
