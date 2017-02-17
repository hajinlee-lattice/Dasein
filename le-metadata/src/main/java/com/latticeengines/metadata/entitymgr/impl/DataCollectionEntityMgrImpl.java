package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableTag;
import com.latticeengines.metadata.dao.DataCollectionDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.TableTagEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataCollectionEntityMgr")
public class DataCollectionEntityMgrImpl extends BaseEntityMgrImpl<DataCollection> implements DataCollectionEntityMgr {

    @Autowired
    private DataCollectionDao dataCollectionDao;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private TableTagEntityMgr tableTagEntityMgr;

    @Override
    public DataCollectionDao getDao() {
        return dataCollectionDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public DataCollection createDataCollection(List<String> tableNames, String statisticsId, boolean isDefault) {
        if (isDefault && getDefaultDataCollection() != null) {
            throw new LedpException(LedpCode.LEDP_11007);
        }

        List<Table> tables = tableNames.stream().map(tableName -> tableEntityMgr.findByName(tableName))
                .collect(Collectors.toList());
        if (tables.stream().anyMatch(table -> table == null)) {
            throw new LedpException(LedpCode.LEDP_11006, new String[] { String.join(",", tableNames) });
        }

        DataCollection dataCollection = new DataCollection();

        for (Table table : tables) {
            TableTag tableTag = new TableTag();
            tableTag.setTable(table);
            tableTag.setName(dataCollection.getName());
            tableTag.setTenantId(MultiTenantContext.getTenant().getPid());
            tableTagEntityMgr.create(tableTag);
        }

        dataCollection.setTenant(MultiTenantContext.getTenant());
        dataCollection.setDefault(isDefault);

        create(dataCollection);
        return getDataCollection(dataCollection.getName());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataCollection getDefaultDataCollection() {
        List<DataCollection> candidates = dataCollectionDao.findAllByField("isDefault", true);

        if (candidates.size() == 0) {
            return null;
        }
        DataCollection dataCollection = candidates.get(0);
        fillInTables(dataCollection);
        return dataCollection;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataCollection getDataCollection(String name) {
        List<DataCollection> candidates = dataCollectionDao.findAllByField("name", name);
        if (candidates.size() == 0) {
            return null;
        }
        DataCollection dataCollection = candidates.get(0);
        fillInTables(dataCollection);
        return dataCollection;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void removeDataCollection(String name) {
        DataCollection dataCollection = dataCollectionDao.findByField("name", name);

        // TODO Remove tags

        dataCollectionDao.delete(dataCollection);
    }

    private void fillInTables(DataCollection dataCollection) {
        List<Table> tables = tableTagEntityMgr.getTablesForTag(dataCollection.getName());
        dataCollection.setTables(tables);
    }

}
