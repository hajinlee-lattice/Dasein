package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.ExportFieldMetadataMappingEntityMgr;
import com.latticeengines.apps.cdl.service.ExportFieldMetadataDefaultsService;
import com.latticeengines.apps.cdl.service.ExportFieldMetadataService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

import reactor.core.publisher.Flux;

@Component
public abstract class ExportFieldMetadataServiceBase implements ExportFieldMetadataService {

    @Inject
    private ExportFieldMetadataDefaultsService defaultExportFieldMetadataService;

    @Inject
    private ExportFieldMetadataMappingEntityMgr exportFieldMetadataMappingEntityMgr;

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

    protected Map<String, ExportFieldMetadataDefaults> getDefaultExportFieldsMapForEntity(
            CDLExternalSystemName systemName, BusinessEntity entity) {
        return defaultExportFieldMetadataService.getExportEnabledAttributesForEntity(systemName, entity).stream()
                .collect(Collectors.toMap(ExportFieldMetadataDefaults::getAttrName, Function.identity()));
    }

    /*
     * This method uses the defaultFieldsMetadataMap (defined by PM) to
     * overwrite the DisplayNames for the attributes chose by user at the field
     * mapping page for the system/connector. If the attribute is not in the
     * serving store, then it is a campaign-derived attribute
     * 
     * @param fieldNames Lattice internal name for attributes that are chosen at
     * field mapping page for the system
     * 
     * @param accountAttributesMap account attributes from serving store
     * 
     * @param contactAttributesMap contact attributes from serving store
     * 
     * One important thing to note that since fieldNames only has the
     * information about internal name without the entity, currently PM has
     * agreed that for the csv sheet, there is no situation where the Account
     * and Contact with same internal name are both in it.
     */
    protected List<ColumnMetadata> enrichExportFieldMappings(CDLExternalSystemName systemName, List<String> fieldNames,
            Map<String, ColumnMetadata> accountAttributesMap, Map<String, ColumnMetadata> contactAttributesMap) {

        List<ColumnMetadata> exportColumnMetadataList = new ArrayList<>();
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
                Collections.singletonList(BusinessEntity.Account));

        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Contact));

        return enrichDefaultFieldsMetadata(systemName, accountAttributesMap, contactAttributesMap);
    }

    /*
     * Enrich the default field metadata based on the entity
     */
    protected List<ColumnMetadata> enrichDefaultFieldsMetadata(CDLExternalSystemName systemName,
            Map<String, ColumnMetadata> accountAttributesMap, Map<String, ColumnMetadata> contactAttributesMap,
            BusinessEntity entity) {

        List<ColumnMetadata> exportColumnMetadataList = new ArrayList<>();
        Map<String, ExportFieldMetadataDefaults> defaultFieldsMetadataMap = getDefaultExportFieldsMapForEntity(
                systemName, entity);

        defaultFieldsMetadataMap.values().forEach(defaultField -> {
            String attrName = defaultField.getAttrName();
            ColumnMetadata cm;
            if (BusinessEntity.Account.equals(entity)) {
                if (defaultField.getStandardField() && accountAttributesMap.containsKey(attrName)) {
                    cm = accountAttributesMap.get(attrName);
                    cm.setDisplayName(defaultField.getDisplayName());
                    accountAttributesMap.remove(attrName);
                } else {
                    cm = constructCampaignDerivedColumnMetadata(defaultField);
                }
            } else if (BusinessEntity.Contact.equals(entity)) {
                if (defaultField.getStandardField() && contactAttributesMap.containsKey(attrName)) {
                    cm = contactAttributesMap.get(attrName);
                    cm.setDisplayName(defaultField.getDisplayName());
                    contactAttributesMap.remove(attrName);
                } else {
                    cm = constructCampaignDerivedColumnMetadata(defaultField);
                }
            } else {
                throw new RuntimeException("entity can either be Account or Contact");
            }

            exportColumnMetadataList.add(cm);
        });

        return exportColumnMetadataList;
    }

    /*
     * Enrich the default field metadata for both account and contact entity
     */
    protected List<ColumnMetadata> enrichDefaultFieldsMetadata(CDLExternalSystemName systemName,
            Map<String, ColumnMetadata> accountAttributesMap, Map<String, ColumnMetadata> contactAttributesMap) {

        List<ColumnMetadata> exportColumnMetadataList = new ArrayList<>();
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
        if (CollectionUtils.isNotEmpty(entities)) {
            List<ColumnMetadata> cmList;
            if (entities.contains(BusinessEntity.Contact)) {
                cmList = servingStoreService.getContactMetadata(customerSpace, ColumnSelection.Predefined.Enrichment,
                        null);
            } else {
                cmList = servingStoreService.getAccountMetadata(customerSpace, ColumnSelection.Predefined.Enrichment,
                        null);
            }
            if (CollectionUtils.isNotEmpty(cmList)) {
                cms = Flux.fromIterable(cmList).filter(
                        cm -> entities.contains(cm.getEntity()) && !AttrState.Inactive.equals(cm.getAttrState()));
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

    protected List<String> getMappedFieldNames(String orgId, Long tenantPid) {
        List<ExportFieldMetadataMapping> mapping = exportFieldMetadataMappingEntityMgr.findByOrgId(orgId, tenantPid);
        return mapping.stream().map(ExportFieldMetadataMapping::getSourceField).collect(Collectors.toList());
    }

}
