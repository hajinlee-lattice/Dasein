package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("marketoExportFieldMetadataService")
public class MarketoExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static final Logger log = LoggerFactory.getLogger(MarketoExportFieldMetadataServiceImpl.class);

    private static final String TRAY_ACCOUNT_ID_COLUMN_NAME = "SFDC ID";

    protected MarketoExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.Marketo);
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling MarketoExportFieldMetadataService for channle " + channel.getId());

        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Account));

        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Contact));

        List<String> mappedFieldNames = getMappedFieldNames(channel.getLookupIdMap().getOrgId(),
                channel.getLookupIdMap().getTenant().getPid());

        List<ColumnMetadata> exportColumnMetadataList;

        if (mappedFieldNames != null && mappedFieldNames.size() != 0) {
            exportColumnMetadataList = enrichExportFieldMappings(CDLExternalSystemName.Marketo, mappedFieldNames,
                    accountAttributesMap, contactAttributesMap);
        } else {
            exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.Marketo, accountAttributesMap,
                    contactAttributesMap);
        }

        String lookupId = channel.getLookupIdMap().getAccountId();
        log.info("Marketo lookupId " + lookupId);
        if (lookupId != null && accountAttributesMap.containsKey(lookupId)) {
            ColumnMetadata lookupIdColumnMetadata = accountAttributesMap.get(lookupId);
            lookupIdColumnMetadata.setDisplayName(TRAY_ACCOUNT_ID_COLUMN_NAME);
            exportColumnMetadataList.add(lookupIdColumnMetadata);
        }

        return exportColumnMetadataList;

    }

}
