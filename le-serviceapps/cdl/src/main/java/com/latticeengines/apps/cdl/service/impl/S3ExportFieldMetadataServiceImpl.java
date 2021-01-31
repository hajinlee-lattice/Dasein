package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.Play;
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
        Play play = channel.getPlay();
        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Account), channelConfig.getAttributeSetName(), play);
        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Contact), channelConfig.getAttributeSetName(), play);
        Map<String, String> defaultFieldsAttributesToServingStoreAttributesRemap = getDefaultFieldsAttrToServingStoreAttrRemap(
                customerSpace, channel);
        List<ColumnMetadata> exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.AWS_S3,
                accountAttributesMap, contactAttributesMap, defaultFieldsAttributesToServingStoreAttributesRemap, channelAudienceType);
        if (channelConfig.isIncludeExportAttributes() && !Play.TapType.ListSegment.equals(play.getTapType())) {
            includeExportAttributes(customerSpace, channelConfig, accountAttributesMap, contactAttributesMap,
                    exportColumnMetadataList, channelAudienceType);
        }
        return exportColumnMetadataList;
    }

    @Override
    protected Map<String, String> getDefaultFieldsAttrToServingStoreAttrRemap(
            String customerSpace,
            PlayLaunchChannel channel) {
        Map<String, String> remappingMap = new HashMap<>();
        String accountId = channel.getLookupIdMap().getAccountId();
        log.info("S3 accountId " + accountId);
        if (!StringUtils.isEmpty(accountId)) {
            remappingMap.put(SFDC_ACCOUNT_ID_INTERNAL_NAME, accountId);
        } else {
            remappingMap.put(SFDC_ACCOUNT_ID_INTERNAL_NAME, getDefaultAccountIdForTenant(customerSpace));

            if (getDefaultAccountIdForTenant(customerSpace).equals(InterfaceName.AccountId.name())) {
                remappingMap.put(InterfaceName.AccountId.name(), SFDC_ACCOUNT_ID_INTERNAL_NAME);
            }
        }

        String contactId = channel.getLookupIdMap().getContactId();
        log.info("S3 contactId " + contactId);
        if (!StringUtils.isEmpty(contactId)) {
            remappingMap.put(SFDC_CONTACT_ID_INTERNAL_NAME, contactId);
        } else {
            remappingMap.put(SFDC_CONTACT_ID_INTERNAL_NAME, getDefaultContactIdForTenant(customerSpace));

            if (getDefaultContactIdForTenant(customerSpace).equals(InterfaceName.ContactId.name())) {
                remappingMap.put(InterfaceName.ContactId.name(), SFDC_CONTACT_ID_INTERNAL_NAME);
            }
        }
        return remappingMap;
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
