package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionProperty;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableTag;
import com.latticeengines.metadata.dao.DataCollectionDao;
import com.latticeengines.metadata.dao.DataCollectionPropertyDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.TableTagEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataCollectionEntityMgr")
public class DataCollectionEntityMgrImpl extends BaseEntityMgrImpl<DataCollection>
        implements DataCollectionEntityMgr {

    @Autowired
    private DataCollectionDao dataCollectionDao;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private TableTagEntityMgr tableTagEntityMgr;

    @Autowired
    DataCollectionPropertyDao dataCollectionPropertyDao;

    @Override
    public DataCollectionDao getDao() {
        return dataCollectionDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public DataCollection createDataCollection(DataCollection dataCollection) {
        List<String> tableNames = dataCollection.getTables().stream() //
                .map(Table::getName).collect(Collectors.toList());
        List<Table> tables = tableNames.stream() //
                .map(name -> tableEntityMgr.findByName(name)) //
                .collect(Collectors.toList());
        if (tables.stream().anyMatch(table -> table == null)) {
            throw new LedpException(LedpCode.LEDP_11006,
                    new String[] { String.join(",", tableNames) });
        }
        dataCollection.setName("DataCollection_" + UUID.randomUUID());

        for (Table table : tables) {
            TableTag tableTag = new TableTag();
            tableTag.setTable(table);
            tableTag.setName(dataCollection.getName());
            tableTag.setTenantId(MultiTenantContext.getTenant().getPid());
            tableTagEntityMgr.create(tableTag);
        }
        dataCollection.setTenant(MultiTenantContext.getTenant());
        create(dataCollection);
        for (DataCollectionProperty dataCollectionProperty : dataCollection.getProperties()) {
            dataCollectionPropertyDao.create(dataCollectionProperty);
        }

        return getDataCollection(dataCollection.getName());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollection getDataCollection(String name) {
        List<DataCollection> candidates = dataCollectionDao.findAllByField("name", name);
        if (candidates.size() == 0) {
            return null;
        }
        DataCollection dataCollection = candidates.get(0);
        HibernateUtils.inflateDetails(dataCollection.getProperties());
        fillInTables(dataCollection);
        return dataCollection;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollection getDataCollection(DataCollectionType type) {
        DataCollection collection = findByField("type", type);
        if (collection != null) {
            fillInTables(collection);
            HibernateUtils.inflateDetails(collection.getProperties());
        }
        return collection;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void removeDataCollection(String name) {
        DataCollection dataCollection = dataCollectionDao.findByField("name", name);

        for (TableTag tableTag : tableTagEntityMgr.getTableTagsForName(name)) {
            tableTagEntityMgr.delete(tableTag);
        }

        dataCollectionDao.delete(dataCollection);
    }

    private void fillInTables(DataCollection dataCollection) {
        List<Table> tables = tableTagEntityMgr.getTablesForTag(dataCollection.getName());
        dataCollection.setTables(tables);
    }

}
