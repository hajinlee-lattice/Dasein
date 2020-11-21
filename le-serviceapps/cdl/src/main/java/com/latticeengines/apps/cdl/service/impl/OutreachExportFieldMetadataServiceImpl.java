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
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("outreachExportFieldMetadataService")
public class OutreachExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static final Logger log = LoggerFactory.getLogger(OutreachExportFieldMetadataServiceImpl.class);

    protected OutreachExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.Outreach);
    }

    private static final String TRAY_PROSPECT_OWNER_COLUMN_NAME = "SDR Email";
    private static final String TRAY_ACCOUNT_ID_COLUMN_NAME = "SFDC Account ID";

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling OutreachExportFieldMetadataService for channel " + channel.getId());

        Map<String, String> defaultFieldsAttrToServingStoreAttrRemap = new HashMap<>();

        List<String> mappedFieldNames = getMappedFieldNames(channel.getLookupIdMap().getOrgId(),
                channel.getLookupIdMap().getTenant().getPid());

        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Account));

        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Contact));

        List<ColumnMetadata> exportColumnMetadataList;

        if (mappedFieldNames != null && mappedFieldNames.size() != 0) {
            exportColumnMetadataList = enrichExportFieldMappings(CDLExternalSystemName.Outreach, mappedFieldNames,
                    accountAttributesMap, contactAttributesMap);
        } else {
            exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.Outreach, accountAttributesMap,
                    contactAttributesMap, defaultFieldsAttrToServingStoreAttrRemap);
        }

        // Retrieves enriched fields for prospect owner and account Id and
        // update
        // displayName
        String prospectOwner = channel.getLookupIdMap().getProspectOwner();
        log.info("Outreach account owner " + prospectOwner);
        if (StringUtils.isNotBlank(prospectOwner) && accountAttributesMap.containsKey(prospectOwner)) {
            ColumnMetadata prospectOwnerColumnMetadata = accountAttributesMap.get(prospectOwner);
            prospectOwnerColumnMetadata.setDisplayName(TRAY_PROSPECT_OWNER_COLUMN_NAME);
            exportColumnMetadataList.add(prospectOwnerColumnMetadata);
        } else if (StringUtils.isNotBlank(prospectOwner) && !accountAttributesMap.containsKey(prospectOwner)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Outreach Prospect Owner:" + prospectOwner + " mapped is not export enabled" });
        }

        String lookupId = channel.getLookupIdMap().getAccountId();
        log.info("Outreach account owner " + lookupId);
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
