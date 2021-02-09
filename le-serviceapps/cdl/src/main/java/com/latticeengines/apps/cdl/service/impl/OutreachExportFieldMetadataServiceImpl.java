package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("outreachExportFieldMetadataService")
public class OutreachExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static final Logger log = LoggerFactory.getLogger(OutreachExportFieldMetadataServiceImpl.class);

    protected OutreachExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.Outreach);
    }

    private static final String TRAY_PROSPECT_OWNER_COLUMN_NAME = "SDR Email";
    private static final String TRAY_ACCOUNT_ID_COLUMN_NAME = "Account ID";

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling OutreachExportFieldMetadataService for channel " + channel.getId());

        OutreachChannelConfig channelConfig = (OutreachChannelConfig) channel.getChannelConfig();
        AudienceType audienceType = channelConfig.getAudienceType();
        Map<String, String> defaultFieldsAttrToServingStoreAttrRemap = getDefaultFieldsAttrNameToServingStoreAttrNameMap(
                customerSpace,
                channel);
        List<String> mappedFieldNames = getMappedFieldNames(channel.getLookupIdMap().getOrgId(),
                channel.getLookupIdMap().getTenant().getPid());
        Play play = channel.getPlay();
        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Account), play);
        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Contact), play);
        List<ColumnMetadata> exportColumnMetadataList;
        if (audienceType == AudienceType.ACCOUNTS) {
            exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.Outreach, accountAttributesMap,
                    contactAttributesMap, defaultFieldsAttrToServingStoreAttrRemap, audienceType);
        } else if (mappedFieldNames != null && mappedFieldNames.size() != 0) {
            exportColumnMetadataList = enrichExportFieldMappings(CDLExternalSystemName.Outreach, mappedFieldNames,
                    accountAttributesMap, contactAttributesMap, defaultFieldsAttrToServingStoreAttrRemap);
        } else {
            exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.Outreach, accountAttributesMap,
                    contactAttributesMap, defaultFieldsAttrToServingStoreAttrRemap);
        }
        // Retrieves enriched fields for prospect owner and account Id and update displayName
        String prospectOwner = channel.getLookupIdMap().getProspectOwner();
        log.info("Outreach account owner: " + prospectOwner);
        if (StringUtils.isNotBlank(prospectOwner) && accountAttributesMap.containsKey(prospectOwner)) {
            ColumnMetadata prospectOwnerColumnMetadata = accountAttributesMap.get(prospectOwner);
            prospectOwnerColumnMetadata.setDisplayName(TRAY_PROSPECT_OWNER_COLUMN_NAME);
            exportColumnMetadataList.add(prospectOwnerColumnMetadata);
        } else if (StringUtils.isNotBlank(prospectOwner) && !accountAttributesMap.containsKey(prospectOwner)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Outreach Prospect Owner:" + prospectOwner + " mapped is not export enabled" });
        }
        String lookupId = channel.getLookupIdMap().getAccountId();
        log.info("Outreach account ID: " + lookupId);
        if (StringUtils.isNotBlank(lookupId) && accountAttributesMap.containsKey(lookupId)) {
            ColumnMetadata lookupIdColumnMetadata = accountAttributesMap.get(lookupId);
            lookupIdColumnMetadata.setDisplayName(TRAY_ACCOUNT_ID_COLUMN_NAME);
            exportColumnMetadataList.add(lookupIdColumnMetadata);
        } else if (StringUtils.isNotBlank(lookupId) && !accountAttributesMap.containsKey(lookupId)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Outreach AccountId:" + lookupId + " mapped is not export enabled" });
        }
        return exportColumnMetadataList;
    }
}
