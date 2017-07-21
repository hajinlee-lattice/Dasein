package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.playmaker.service.LpiPMAccountExtension;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.AccountProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("lpiPMAccountExtension")
public class LpiPMAccountExtensionImpl implements LpiPMAccountExtension {

    @Autowired
    private AccountProxy accountProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum, List<String> accountIds,
            Long recStart, String columns, boolean hasSfdcContactId) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataRequest dataRequest = new DataRequest();
        dataRequest.setAccountIds(accountIds);
        List<String> attributes = new ArrayList<>();
        dataRequest.setAttributes(attributes);
        DataPage dataPage = accountProxy.getAccounts(customerSpace,
                DateTimeUtils.convertToStringUTCISO8601(new Date(start)), offset, maximum, dataRequest);

        return dataPage.getData();
    }

    @Override
    public int getAccountExtensionCount(long start, List<String> accountIds, Long recStart) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataRequest dataRequest = new DataRequest();
        dataRequest.setAccountIds(accountIds);
        return (int) accountProxy.getAccountsCount(customerSpace,
                DateTimeUtils.convertToStringUTCISO8601(new Date(start)), dataRequest);
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema() {
        return getSchema(TableRoleInCollection.BucketedAccount);
    }

    @Override
    public List<Map<String, Object>> getContactExtensionSchema() {
        return getSchema(TableRoleInCollection.SortedContact);
    }

    @Override
    public int getAccountExtensionColumnCount() {
        List<Attribute> schemaAttributes = getSchemaAttributes(TableRoleInCollection.BucketedAccount);
        return schemaAttributes.size();
    }

    @Override
    public int getContactExtensionColumnCount() {
        List<Attribute> schemaAttributes = getSchemaAttributes(TableRoleInCollection.SortedContact);
        return schemaAttributes.size();
    }

    private List<Map<String, Object>> getSchema(TableRoleInCollection role) {
        List<Attribute> schemaAttributes = getSchemaAttributes(role);

        Stream<Map<String, Object>> stream = schemaAttributes.stream() //
                .map(Attribute::getColumnMetadata) //
                .sorted(Comparator.comparing(ColumnMetadata::getColumnId)) //
                .map(metadata -> {
                    Map<String, Object> metadataInfoMap = new HashMap<>();
                    metadataInfoMap.put("DisplayName", metadata.getDisplayName());
                    metadataInfoMap.put("Type", metadata.getDataType());
                    metadataInfoMap.put("JavaType", metadata.getJavaClass());
                    metadataInfoMap.put("StringLength", 4000);
                    metadataInfoMap.put("field", metadata.getColumnId());
                    return metadataInfoMap;
                });

        return stream.collect(Collectors.toList());
    }

    private List<Attribute> getSchemaAttributes(TableRoleInCollection role) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        Table schemaTable = dataCollectionProxy.getTable(customerSpace, role);
        List<Attribute> schemaAttributes = schemaTable.getAttributes();
        return schemaAttributes;
    }
}
