package com.latticeengines.playmaker.dao.impl;

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

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.playmaker.dao.LpiPMAccountExtensionDao;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.AccountProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("lpiPMAccountExtensionDao")
public class LpiPMAccountExtensionDaoImpl implements LpiPMAccountExtensionDao {

    @Autowired
    private AccountProxy accountProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum, List<String> accountIds,
            Long recStart, String columns, boolean hasSfdcContactId) {
        String customerSpace = "";
        DataRequest dataRequest = new DataRequest();
        dataRequest.setAccountIds(accountIds);
        List<String> attributes = new ArrayList<>();
        dataRequest.setAttributes(attributes);
        DataPage dataPage = accountProxy.getAccounts(customerSpace, (new Date(start)).toGMTString(), offset, maximum,
                dataRequest);

        return dataPage.getData();
    }

    @Override
    public int getAccountExtensionCount(long start, List<String> accountIds, Long recStart) {
        String customerSpace = "";
        DataRequest dataRequest = new DataRequest();
        dataRequest.setAccountIds(accountIds);
        return (int) accountProxy.getAccountsCount(customerSpace, (new Date(start)).toGMTString(), dataRequest);
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
        String customerSpace = MultiTenantContext.getTenant().getId();
        Table schemaTable = dataCollectionProxy.getTable(customerSpace, role);
        List<Attribute> schemaAttributes = schemaTable.getAttributes();
        return schemaAttributes;
    }
}
