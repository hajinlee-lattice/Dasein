package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("s3ExportFieldMetadataService")
public class S3ExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static final Logger log = LoggerFactory.getLogger(S3ExportFieldMetadataServiceImpl.class);

    private static final String SFDC_ACCOUNT_ID_INTERNAL_NAME = "SFDC_ACCOUNT_ID";

    private static final String SFDC_CONTACT_ID_INTERNAL_NAME = "SFDC_CONTACT_ID";

    protected S3ExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.AWS_S3);
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling S3ExportFieldMetadataService for channel " + channel.getId());
        S3ChannelConfig channelConfig = (S3ChannelConfig) channel.getChannelConfig();
        AudienceType channelAudienceType = channelConfig.getAudienceType();

        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Account), channelConfig.getAttributeSetName());

        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Contact), channelConfig.getAttributeSetName());

        Map<String, ExportFieldMetadataDefaults> defaultFieldsMetadataMap = getDefaultExportFieldsMapForAudienceType(
                CDLExternalSystemName.AWS_S3, channelAudienceType);

        Map<String, String> defaultFieldsAttributesToServingStoreAttributesRemap = generateRemappingMap(channel);

        List<ColumnMetadata> exportColumnMetadataList = combineAttributeMapsWithDefaultFields(accountAttributesMap,
                contactAttributesMap, defaultFieldsMetadataMap, defaultFieldsAttributesToServingStoreAttributesRemap);

        if (channelConfig.isIncludeExportAttributes()) {
            includeExportAttributes(customerSpace, channelConfig, accountAttributesMap, contactAttributesMap,
                    exportColumnMetadataList,
                    channelAudienceType);
        }

        return exportColumnMetadataList;
    }

    private Map<String, String> generateRemappingMap(PlayLaunchChannel channel) {
        Map<String, String> remappingMap = new HashMap<>();

        String contactId = channel.getLookupIdMap().getContactId();
        String accountId = channel.getLookupIdMap().getAccountId();
        log.info("S3 contactId " + contactId);
        log.info("S3 accountId " + accountId);

        putIfValueNotNull(remappingMap, SFDC_CONTACT_ID_INTERNAL_NAME, contactId);
        putIfValueNotNull(remappingMap, SFDC_ACCOUNT_ID_INTERNAL_NAME, accountId);

        return remappingMap;
    }

    private void putIfValueNotNull(Map<String, String> map, String key, String value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    protected List<ColumnMetadata> combineAttributeMapsWithDefaultFields(
            Map<String, ColumnMetadata> accountAttributesMap,
            Map<String, ColumnMetadata> contactAttributesMap,
            Map<String, ExportFieldMetadataDefaults> defaultFieldsMetadataMap,
            Map<String, String> defaultFieldsAttributesToServingStoreAttributesRemap) {

        List<ColumnMetadata> exportColumnMetadataList = new ArrayList<>();

        defaultFieldsMetadataMap.values().forEach(defaultField -> {
            String attrName = defaultField.getAttrName();
            ColumnMetadata cm = null;

            if (defaultField.getStandardField()) {
                if (defaultField.getEntity() != BusinessEntity.Contact) {
                    if (defaultFieldsAttributesToServingStoreAttributesRemap.containsKey(attrName)) {
                        attrName = defaultFieldsAttributesToServingStoreAttributesRemap.get(attrName);
                    }
                    if (accountAttributesMap.containsKey(attrName)) {
                        cm = accountAttributesMap.get(attrName);
                        cm.setDisplayName(defaultField.getDisplayName());
                        accountAttributesMap.remove(attrName);
                    }
                }
                if (defaultField.getEntity() == BusinessEntity.Contact) {
                    if (defaultFieldsAttributesToServingStoreAttributesRemap.containsKey(attrName)) {
                        attrName = defaultFieldsAttributesToServingStoreAttributesRemap.get(attrName);
                    }
                    if (contactAttributesMap.containsKey(attrName)) {
                        cm = contactAttributesMap.get(attrName);
                        cm.setDisplayName(defaultField.getDisplayName());
                        contactAttributesMap.remove(attrName);
                    }
                }
            }

            if (cm == null) {
                cm = constructCampaignDerivedColumnMetadata(defaultField);
            }
            exportColumnMetadataList.add(cm);
        });

        return exportColumnMetadataList;
    }

    private void includeExportAttributes(String customerSpace, S3ChannelConfig channelConfig,
            Map<String, ColumnMetadata> accountAttributesMap, Map<String, ColumnMetadata> contactAttributesMap,
            List<ColumnMetadata> exportColumnMetadataList, AudienceType channelAudienceType) {
        if (channelAudienceType == AudienceType.CONTACTS) {
            exportColumnMetadataList.addAll(contactAttributesMap.values());
            List<BusinessEntity> contactEntities = BusinessEntity.EXPORT_CONTACT_ENTITIES.stream().filter(entity -> !BusinessEntity.Contact.equals(entity)).collect(Collectors.toList());
            exportColumnMetadataList.addAll(getServingMetadata(customerSpace, contactEntities, channelConfig.getAttributeSetName()).collect(Collectors.toList()).block());
        }
        exportColumnMetadataList.addAll(accountAttributesMap.values());
        List<BusinessEntity> accountEntities = BusinessEntity.EXPORT_ACCOUNT_ENTITIES.stream().filter(entity -> !BusinessEntity.Account.equals(entity)).collect(Collectors.toList());
        exportColumnMetadataList.addAll(getServingMetadata(customerSpace, accountEntities, channelConfig.getAttributeSetName()).collect(Collectors.toList()).block());

    }
}
