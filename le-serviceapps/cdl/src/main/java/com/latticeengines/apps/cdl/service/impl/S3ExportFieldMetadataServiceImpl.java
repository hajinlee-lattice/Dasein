package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("s3ExportFieldMetadataService")
public class S3ExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static final Logger log = LoggerFactory.getLogger(S3ExportFieldMetadataServiceImpl.class);

    private static final String TRAY_ACCOUNT_ID_COLUMN_NAME = "SFDC Account ID";

    private static final String TRAY_CONTACT_ID_COLUMN_NAME = "SFDC Contact ID";

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

        List<ColumnMetadata> exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.AWS_S3,
                accountAttributesMap, contactAttributesMap, channelAudienceType);

        if (channelAudienceType.equals(AudienceType.CONTACTS)) {
            enrichS3ContactsFields(customerSpace, channel, channelConfig,
                    exportColumnMetadataList,
                    contactAttributesMap);
            enrichS3AccountsFields(customerSpace, channel, channelConfig,
                    exportColumnMetadataList,
                    accountAttributesMap);
        } else if (channelAudienceType.equals(AudienceType.ACCOUNTS)) {
            enrichS3AccountsFields(customerSpace, channel, channelConfig,
                    exportColumnMetadataList,
                    accountAttributesMap);
        }

        return exportColumnMetadataList;
    }

    private void enrichS3ContactsFields(String customerSpace,
            PlayLaunchChannel channel,
            S3ChannelConfig channelConfig, List<ColumnMetadata> exportColumnMetadataList,
            Map<String, ColumnMetadata> contactAttributesMap) {
        String contactId = channel.getLookupIdMap().getContactId();
        log.info("S3 contactId " + contactId);
        renameAndAddColumn(contactId, TRAY_CONTACT_ID_COLUMN_NAME, contactAttributesMap, exportColumnMetadataList);

        if (channelConfig.isIncludeExportAttributes()) {
            exportColumnMetadataList.addAll(contactAttributesMap.values());
        }
    }

    private void enrichS3AccountsFields(String customerSpace,
            PlayLaunchChannel channel,
            S3ChannelConfig channelConfig, List<ColumnMetadata> exportColumnMetadataList,
            Map<String, ColumnMetadata> accountAttributesMap) {
        String accountId = channel.getLookupIdMap().getAccountId();
        log.info("S3 accountId " + accountId);
        renameAndAddColumn(accountId, TRAY_ACCOUNT_ID_COLUMN_NAME, accountAttributesMap, exportColumnMetadataList);

        if (channelConfig.isIncludeExportAttributes()) {
            exportColumnMetadataList.addAll(accountAttributesMap.values());
            exportColumnMetadataList.addAll(getServingMetadata(customerSpace, Arrays.asList(BusinessEntity.Rating,
                    BusinessEntity.PurchaseHistory,
                    BusinessEntity.CuratedAccount), channelConfig.getAttributeSetName()).collect(Collectors.toList())
                            .block());
        }
    }

    private void renameAndAddColumn(String oldName, String newName, Map<String, ColumnMetadata> attributesMap,
            List<ColumnMetadata> exportColumnMetadataList) {
        if (oldName != null && attributesMap.containsKey(oldName)) {
            ColumnMetadata columnMetadata = attributesMap.get(oldName);
            columnMetadata.setDisplayName(newName);
            exportColumnMetadataList.add(columnMetadata);
            attributesMap.remove(oldName);
        }
    }
}
