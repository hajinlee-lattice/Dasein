package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
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

    private static final String PROSPECT_OWNER_INTERNAL_NAME = "SDR_Email";
    private static final String SFDC_ACCOUNT_ID_INTERNAL_NAME = "SFDC_ACCOUNT_ID";

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling OutreachExportFieldMetadataService for channel " + channel.getId());

        OutreachChannelConfig channelConfig = (OutreachChannelConfig) channel.getChannelConfig();
        AudienceType audienceType = channelConfig.getAudienceType();
        Map<String, String> defaultFieldsAttrToServingStoreAttrRemap = getDefaultFieldsAttrNameToServingStoreAttrNameMap(
                customerSpace, channel);
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
            mappedFieldNames.add(PROSPECT_OWNER_INTERNAL_NAME);
            mappedFieldNames.add(SFDC_ACCOUNT_ID_INTERNAL_NAME);

            exportColumnMetadataList = enrichExportFieldMappings(CDLExternalSystemName.Outreach, mappedFieldNames,
                    accountAttributesMap, contactAttributesMap, defaultFieldsAttrToServingStoreAttrRemap);
        } else {
            exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.Outreach, accountAttributesMap,
                    contactAttributesMap, defaultFieldsAttrToServingStoreAttrRemap);
        }
        return exportColumnMetadataList;
    }

    @Override
    protected Map<String, String> getDefaultFieldsAttrNameToServingStoreAttrNameMap(
            String customerSpace,
            PlayLaunchChannel channel) {
        Map<String, String> remappingMap = new HashMap<>();

        String accountId = channel.getLookupIdMap().getAccountId();
        log.info("Outreach accountId " + accountId);
        if (!StringUtils.isEmpty(accountId)) {
            remappingMap.put(SFDC_ACCOUNT_ID_INTERNAL_NAME, accountId);
        }

        String prospectOwner = channel.getLookupIdMap().getProspectOwner();
        log.info("Outreach Prospect Owner " + prospectOwner);
        if (!StringUtils.isEmpty(prospectOwner)) {
            remappingMap.put(PROSPECT_OWNER_INTERNAL_NAME, prospectOwner);
        }

        return remappingMap;
    }

}
