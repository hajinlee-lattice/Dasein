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
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.Play.TapType;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
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

    @Inject
    private SegmentService segmentService;

    @Inject
    private BatonService batonService;

    private static Map<CDLExternalSystemName, ExportFieldMetadataService> registry = new HashMap<>();

    private static final String SFDC_ACCOUNT_ID_INTERNAL_NAME = "SFDC_ACCOUNT_ID";
    private static final String SFDC_CONTACT_ID_INTERNAL_NAME = "SFDC_CONTACT_ID";

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

    protected Map<String, ExportFieldMetadataDefaults> getDefaultExportFieldsMapForAudienceType(
            CDLExternalSystemName systemName, AudienceType audienceType) {
        return defaultExportFieldMetadataService.getExportEnabledAttributesForAudienceType(systemName,
                audienceType).stream().collect(Collectors.toMap(ExportFieldMetadataDefaults::getAttrName, Function.identity()));
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
            Map<String, ColumnMetadata> accountAttributesMap, Map<String, ColumnMetadata> contactAttributesMap,
            Map<String, String> defaultFieldsAttrToServingStoreAttrRemap) {
        List<ColumnMetadata> exportColumnMetadataList = new ArrayList<>();
        Map<String, ExportFieldMetadataDefaults> defaultFieldsMetadataMap = getDefaultExportFieldsMap(systemName);
        fieldNames.forEach(fieldName -> {
            ExportFieldMetadataDefaults defaultField = defaultFieldsMetadataMap.get(fieldName);
            if (defaultField == null) {
                throw new LedpException(LedpCode.LEDP_40069, new String[] { fieldName, systemName.name() });
            }

            ColumnMetadata cm = getColumnMetadata(defaultField, accountAttributesMap, contactAttributesMap,
                    defaultFieldsAttrToServingStoreAttrRemap);

            exportColumnMetadataList.add(cm);
        });

        return exportColumnMetadataList;
    }

    protected List<ColumnMetadata> enrichDefaultFieldsMetadata(String customerSpace, PlayLaunchChannel channel) {
        CDLExternalSystemName systemName = channel.getChannelConfig().getSystemName();
        AudienceType audienceType = channel.getChannelConfig().getAudienceType();

        Map<String, String> defaultFieldsAttrToServingStoreAttrRemap = getDefaultFieldsAttrNameToServingStoreAttrNameMap(
                customerSpace,
                channel);
        Play play = channel.getPlay();
        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Account), play);

        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Contact), play);

        return enrichDefaultFieldsMetadata(systemName, accountAttributesMap, contactAttributesMap,
                defaultFieldsAttrToServingStoreAttrRemap, audienceType);
    }

    /*
     * Enrich the default field metadata for both account and contact entity
     */
    protected List<ColumnMetadata> enrichDefaultFieldsMetadata(CDLExternalSystemName systemName,
            Map<String, ColumnMetadata> accountAttributesMap, Map<String, ColumnMetadata> contactAttributesMap,
            Map<String, String> defaultFieldsAttrToServingStoreAttrRemap) {
        Map<String, ExportFieldMetadataDefaults> defaultFieldsMetadataMap = getDefaultExportFieldsMap(systemName);
        return combineAttributeMapsWithDefaultFields(accountAttributesMap, contactAttributesMap,
                defaultFieldsMetadataMap, defaultFieldsAttrToServingStoreAttrRemap);
    }

    /*
     * Enrich the default field metadata for both account and contact entity
     * based on Audience Type
     */
    protected List<ColumnMetadata> enrichDefaultFieldsMetadata(CDLExternalSystemName systemName,
            Map<String, ColumnMetadata> accountAttributesMap, Map<String, ColumnMetadata> contactAttributesMap,
            Map<String, String> defaultFieldsAttrToServingStoreAttrRemap, AudienceType audienceType) {
        Map<String, ExportFieldMetadataDefaults> defaultFieldsMetadataMap = getDefaultExportFieldsMapForAudienceType(
                systemName, audienceType);
        return combineAttributeMapsWithDefaultFields(accountAttributesMap, contactAttributesMap,
                defaultFieldsMetadataMap, defaultFieldsAttrToServingStoreAttrRemap);
    }

    protected List<ColumnMetadata> combineAttributeMapsWithDefaultFields(Map<String, ColumnMetadata> accountAttributesMap,
            Map<String, ColumnMetadata> contactAttributesMap, Map<String, ExportFieldMetadataDefaults> defaultFieldsMetadataMap,
            Map<String, String> defaultFieldsAttrToServingStoreAttrRemap) {
        List<ColumnMetadata> exportColumnMetadataList = new ArrayList<>();
        defaultFieldsMetadataMap.values().forEach(defaultField -> {
            ColumnMetadata cm = getColumnMetadata(defaultField, accountAttributesMap, contactAttributesMap,
                    defaultFieldsAttrToServingStoreAttrRemap);
            exportColumnMetadataList.add(cm);
        });
        return exportColumnMetadataList;
    }

    /*
     * Map (ExportFieldMetadataDefaults.attributeName -> ServingStore.attributeName)
     * Enables us to map ExportFieldMetadataDefaults to specific ServingStore attributes
     * and combine the ExportFieldMetadataDefaults Display Name with ServingStore internal name
     */
    protected Map<String, String> getDefaultFieldsAttrNameToServingStoreAttrNameMap(
            String customerSpace,
            PlayLaunchChannel channel) {
        return Collections.emptyMap();
    }

    protected ColumnMetadata getColumnMetadata(ExportFieldMetadataDefaults defaultField, Map<String, ColumnMetadata> accountAttributesMap,
            Map<String, ColumnMetadata> contactAttributesMap,
            Map<String, String> defaultFieldsAttrToServingStoreAttrRemap) {
        String attrName = defaultField.getAttrName();
        ColumnMetadata cm = null;

        if (defaultField.getStandardField() && defaultField.getEntity() != BusinessEntity.Contact) {
            if (defaultFieldsAttrToServingStoreAttrRemap.containsKey(defaultField.getAttrName())) {
                attrName = defaultFieldsAttrToServingStoreAttrRemap.get(defaultField.getAttrName());
            }
            if (accountAttributesMap.containsKey(attrName)) {
                cm = retrieveServingStoreColumnMetadata(defaultField, attrName, accountAttributesMap);
                accountAttributesMap.remove(attrName);
            } else if (defaultField.getForcePopulateIfExportEnabled()) {
                cm = constructForcePopulateColumnMetadata(defaultField);
            }
        } else if (defaultField.getStandardField() && defaultField.getEntity() == BusinessEntity.Contact) {
            if (defaultFieldsAttrToServingStoreAttrRemap.containsKey(defaultField.getAttrName())) {
                attrName = defaultFieldsAttrToServingStoreAttrRemap.get(defaultField.getAttrName());
            }

            if (contactAttributesMap.containsKey(attrName)) {
                cm = retrieveServingStoreColumnMetadata(defaultField, attrName, contactAttributesMap);
                contactAttributesMap.remove(attrName);
            } else if (defaultField.getForcePopulateIfExportEnabled()) {
                cm = constructForcePopulateColumnMetadata(defaultField);
            }
        }
        if (cm == null) {
            cm = constructCampaignDerivedColumnMetadata(defaultField, attrName);
        }
        return cm;
    }

    protected Flux<ColumnMetadata> getServingMetadata(String customerSpace, List<BusinessEntity> entities, String attributeSetName) {
        Flux<ColumnMetadata> cms = Flux.empty();
        if (CollectionUtils.isNotEmpty(entities)) {
            List<ColumnMetadata> cmList;
            cmList = servingStoreService.getEntitiesMetadata(customerSpace, ColumnSelection.Predefined.Enrichment,
                    entities, attributeSetName, null);
            if (CollectionUtils.isNotEmpty(cmList)) {
                cms = Flux.fromIterable(cmList).filter(
                        cm -> entities.contains(cm.getEntity()) && !AttrState.Inactive.equals(cm.getAttrState()));
            }
        }
        return cms;
    }

    protected Map<String, ColumnMetadata> getServingMetadataMap(String customerSpace, List<BusinessEntity> entities, Play play) {
        return getServingMetadataMap(customerSpace, entities, null, play);
    }

    protected Map<String, ColumnMetadata> getServingMetadataMap(String customerSpace, List<BusinessEntity> entities, String attributeSetName, Play play) {
        if (TapType.ListSegment.equals(play.getTapType())) {
            return segmentService.getListSegmentMetadataMap(play.getTapId(), entities);
        } else {
            return getServingMetadata(customerSpace, entities, attributeSetName)
                    .collect(HashMap<String, ColumnMetadata>::new, (returnMap, cm) -> returnMap.put(cm.getAttrName(), cm)).block();
        }
    }

    protected boolean isDefaultIdFeatureFlagForS3AndSalesforceEnabled(String customerSpace) {
        return batonService.isEnabled(CustomerSpace.parse(customerSpace), LatticeFeatureFlag.ENABLE_IR_DEFAULT_IDS);
    }

    protected String getDefaultAccountIdForTenant(String customerSpace) {
        if (batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace))) {
            return InterfaceName.CustomerAccountId.name();
        }
        
        return SFDC_ACCOUNT_ID_INTERNAL_NAME;
    }

    protected String getDefaultContactIdForTenant(String customerSpace) {
        if (batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace))) {
            return InterfaceName.CustomerContactId.name();
        }

        return SFDC_CONTACT_ID_INTERNAL_NAME;
    }

    protected ColumnMetadata constructCampaignDerivedColumnMetadata(ExportFieldMetadataDefaults defaultExportField, String attrName) {
        ColumnMetadata cm = new ColumnMetadata(attrName, defaultExportField.getJavaClass());
        cm.setDisplayName(defaultExportField.getDisplayName());
        cm.setIsCampaignDerivedField(true);
        cm.setEntity(defaultExportField.getEntity());
        return cm;
    }

    protected ColumnMetadata retrieveServingStoreColumnMetadata(ExportFieldMetadataDefaults defaultExportField,
            String attrNameForLookup, Map<String, ColumnMetadata> servingStoreEntityMap) {
        ColumnMetadata cm = servingStoreEntityMap.get(attrNameForLookup);
        cm.setDisplayName(defaultExportField.getDisplayName());
        return cm;
    }

    protected ColumnMetadata constructForcePopulateColumnMetadata(ExportFieldMetadataDefaults defaultExportField) {
        ColumnMetadata cm = new ColumnMetadata(defaultExportField.getAttrName(), defaultExportField.getJavaClass());
        cm.setDisplayName(defaultExportField.getDisplayName());
        cm.setEntity(defaultExportField.getEntity());
        return cm;
    }

    protected List<String> getMappedFieldNames(String orgId, Long tenantPid) {
        List<ExportFieldMetadataMapping> mapping = exportFieldMetadataMappingEntityMgr.findByOrgId(orgId, tenantPid);
        return mapping.stream().map(ExportFieldMetadataMapping::getSourceField).collect(Collectors.toList());
    }

}
