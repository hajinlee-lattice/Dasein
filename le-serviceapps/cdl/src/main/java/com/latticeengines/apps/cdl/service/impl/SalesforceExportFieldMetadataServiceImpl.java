package com.latticeengines.apps.cdl.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

@Component("salesforceExportFieldMetadataService")
public class SalesforceExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static final Logger log = LoggerFactory.getLogger(SalesforceExportFieldMetadataServiceImpl.class);

    private static final String SFDC_ACCOUNT_ID_INTERNAL_NAME = "SFDC_ACCOUNT_ID";
    private static final String SFDC_CONTACT_ID_INTERNAL_NAME = "SFDC_CONTACT_ID";

    protected SalesforceExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.Salesforce);
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling SalesforceExportFieldMetadataService for channel " + channel.getId());
        return enrichDefaultFieldsMetadata(customerSpace, channel);
    }

    @Override
    protected Map<String, String> getDefaultFieldsAttrNameToServingStoreAttrNameMap(
            String customerSpace,
            PlayLaunchChannel channel) {
        Map<String, String> remappingMap = new HashMap<>();

        String accountId = channel.getLookupIdMap().getAccountId();
        log.info("Salesforce accountId " + accountId);
        if (!StringUtils.isEmpty(accountId)) {
            remappingMap.put(SFDC_ACCOUNT_ID_INTERNAL_NAME, accountId);
        } else {
            remappingMap.put(SFDC_ACCOUNT_ID_INTERNAL_NAME, getDefaultAccountIdForTenant(customerSpace));
        }

        String contactId = channel.getLookupIdMap().getContactId();
        log.info("Salesforce contactId " + contactId);
        if (!StringUtils.isEmpty(contactId)) {
            remappingMap.put(SFDC_CONTACT_ID_INTERNAL_NAME, contactId);
        } else {
            remappingMap.put(SFDC_CONTACT_ID_INTERNAL_NAME, getDefaultContactIdForTenant(customerSpace));
        }
        return remappingMap;
    }
}
