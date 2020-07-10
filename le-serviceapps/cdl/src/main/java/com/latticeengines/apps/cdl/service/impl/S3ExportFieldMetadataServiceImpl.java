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
        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Account), channelConfig.getAttributeSetName());

        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Contact), channelConfig.getAttributeSetName());

        List<ColumnMetadata> exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.AWS_S3,
                accountAttributesMap, contactAttributesMap);

        String accountId = channel.getLookupIdMap().getAccountId();
        log.info("S3 accountId " + accountId);
        if (accountId != null && accountAttributesMap.containsKey(accountId)) {
            ColumnMetadata accountIdColumnMetadata = accountAttributesMap.get(accountId);
            accountIdColumnMetadata.setDisplayName(TRAY_ACCOUNT_ID_COLUMN_NAME);
            exportColumnMetadataList.add(accountIdColumnMetadata);
            accountAttributesMap.remove(accountId);
        }

        String contactId = channel.getLookupIdMap().getContactId();
        log.info("S3 contactId " + contactId);
        if (contactId != null && contactAttributesMap.containsKey(contactId)) {
            ColumnMetadata contactIdColumnMetadata = contactAttributesMap.get(contactId);
            contactIdColumnMetadata.setDisplayName(TRAY_CONTACT_ID_COLUMN_NAME);
            exportColumnMetadataList.add(contactIdColumnMetadata);
            contactAttributesMap.remove(contactId);
        }

        if (channelConfig.isIncludeExportAttributes()) {
            exportColumnMetadataList.addAll(accountAttributesMap.values());
            exportColumnMetadataList.addAll(contactAttributesMap.values());
            exportColumnMetadataList.addAll(getServingMetadata(customerSpace,
                    Arrays.asList(BusinessEntity.Rating, BusinessEntity.PurchaseHistory, BusinessEntity.CuratedAccount), channelConfig.getAttributeSetName())
                    .collect(Collectors.toList()).block());
        }

        return exportColumnMetadataList;
    }
}
