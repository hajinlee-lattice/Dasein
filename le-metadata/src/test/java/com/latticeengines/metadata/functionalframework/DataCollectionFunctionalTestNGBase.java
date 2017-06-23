package com.latticeengines.metadata.functionalframework;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionProperty;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class DataCollectionFunctionalTestNGBase extends MetadataFunctionalTestNGBase {

    @Autowired
    protected DataCollectionEntityMgr dataCollectionEntityMgr;

    protected String collectionName;
    protected DataCollection dataCollection;

    @Override
    protected void setup() {
        super.setup();
        createDataCollection();
    }

    @Override
    protected void cleanup() {
        dataCollection = dataCollectionEntityMgr.getDataCollection(collectionName);
        dataCollectionEntityMgr.delete(dataCollection);
        super.cleanup();
    }

    private void createDataCollection() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        dataCollection = new DataCollection();
        dataCollection.setType(DataCollectionType.Segmentation);
        List<DataCollectionProperty> properties = new ArrayList<>();
        properties.add(new DataCollectionProperty("key1", "value1"));
        properties.add(new DataCollectionProperty("key2", "value2"));
        dataCollection.setProperties(properties);
        dataCollection = dataCollectionEntityMgr.createDataCollection(dataCollection);
        collectionName = dataCollection.getName();
    }

    protected void addTableToCollection(Table table) {
        addTableToCollection(table, TableRoleInCollection.ConsolidatedAccount);
    }

    protected void addTableToCollection(Table table, TableRoleInCollection role) {
        if (tableEntityMgr.findByName(table.getName()) == null) {
            tableEntityMgr.create(table);
        }
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, table.getName(), role);
    }

    protected List<Table> getTablesInCollection() {
        return dataCollectionEntityMgr.getTablesOfRole(collectionName, null);
    }

}
