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
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("marketoExportFieldMetadataService")
public class MarketoExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static final Logger log = LoggerFactory.getLogger(MarketoExportFieldMetadataServiceImpl.class);

    private static final String SFDC_ACCOUNT_ID_INTERNAL_NAME = "SFDC_ACCOUNT_ID";
    private static final String SFDC_CONTACT_ID_INTERNAL_NAME = "SFDC_CONTACT_ID";

    protected MarketoExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.Marketo);
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling MarketoExportFieldMetadataService for channel " + channel.getId());
        Map<String, String> defaultFieldsAttrToServingStoreAttrRemap = getDefaultFieldsAttrNameToServingStoreAttrNameMap(
                customerSpace, channel);
        Play play = channel.getPlay();
        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Account), play);
        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Collections.singletonList(BusinessEntity.Contact), play);
        List<String> mappedFieldNames = getMappedFieldNames(channel.getLookupIdMap().getOrgId(),
                channel.getLookupIdMap().getTenant().getPid());
        List<ColumnMetadata> exportColumnMetadataList;
        if (mappedFieldNames != null && mappedFieldNames.size() != 0) {
            mappedFieldNames.add(SFDC_ACCOUNT_ID_INTERNAL_NAME);
            mappedFieldNames.add(SFDC_CONTACT_ID_INTERNAL_NAME);

            exportColumnMetadataList = enrichExportFieldMappings(CDLExternalSystemName.Marketo, mappedFieldNames,
                    accountAttributesMap, contactAttributesMap, defaultFieldsAttrToServingStoreAttrRemap);
        } else {
            exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.Marketo, accountAttributesMap,
                    contactAttributesMap, defaultFieldsAttrToServingStoreAttrRemap);
        }
        return exportColumnMetadataList;
    }

    @Override
    protected Map<String, String> getDefaultFieldsAttrNameToServingStoreAttrNameMap(String customerSpace,
            PlayLaunchChannel channel) {
        Map<String, String> remappingMap = new HashMap<>();

        String accountId = channel.getLookupIdMap().getAccountId();
        log.info("Marketo accountId " + accountId);
        if (!StringUtils.isEmpty(accountId)) {
            remappingMap.put(SFDC_ACCOUNT_ID_INTERNAL_NAME, accountId);
        } else {
            if (!getDefaultAccountIdForTenant(customerSpace).equals(InterfaceName.AccountId.name())) {
                remappingMap.put(SFDC_ACCOUNT_ID_INTERNAL_NAME, getDefaultAccountIdForTenant(customerSpace));
            }
        }

        String contactId = channel.getLookupIdMap().getContactId();
        log.info("Marketo contactId " + contactId);
        if (!StringUtils.isEmpty(contactId)) {
            remappingMap.put(SFDC_CONTACT_ID_INTERNAL_NAME, contactId);
        } else {
            remappingMap.put(SFDC_CONTACT_ID_INTERNAL_NAME, getDefaultContactIdForTenant(customerSpace));
        }
        return remappingMap;
    }

}
