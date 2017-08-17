package com.latticeengines.metadata.functionalframework;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.metadata.DataCollection;
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

    private void createDataCollection() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        dataCollection = dataCollectionEntityMgr.getOrCreateDefaultCollection();
        collectionName = dataCollection.getName();
    }

    protected void addTableToCollection(Table table, TableRoleInCollection role) {
        DataCollection.Version version = dataCollectionEntityMgr.getActiveVersion();
        if (tableEntityMgr.findByName(table.getName()) == null) {
            tableEntityMgr.create(table);
        }
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, table.getName(), role, version);
    }

    protected List<Table> getTablesInCollection() {
        DataCollection.Version version = dataCollectionEntityMgr.getActiveVersion();
        return dataCollectionEntityMgr.getTablesOfRole(collectionName, null, version);
    }

}
