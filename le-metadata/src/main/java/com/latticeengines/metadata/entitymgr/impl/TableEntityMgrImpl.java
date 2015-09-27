package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.dao.ExtractDao;
import com.latticeengines.metadata.dao.LastModifiedKeyDao;
import com.latticeengines.metadata.dao.PrimaryKeyDao;
import com.latticeengines.metadata.dao.TableDao;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("tableEntityMgr")
public class TableEntityMgrImpl extends BaseEntityMgrImpl<Table> implements TableEntityMgr {

    @Autowired
    private AttributeDao attributeDao;

    @Autowired
    private ExtractDao extractDao;
    
    @Autowired
    private LastModifiedKeyDao lastModifiedKeyDao;

    @Autowired
    private PrimaryKeyDao primaryKeyDao;

    @Autowired
    private TableDao tableDao;

    public BaseDao<Table> getDao() {
        return tableDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void create(Table entity) {
        primaryKeyDao.create(entity.getPrimaryKey());
        lastModifiedKeyDao.create(entity.getLastModifiedKey());
        getDao().create(entity);
        for (Extract extract : entity.getExtracts()) {
            extractDao.create(extract);
        }

        for (Attribute attr : entity.getAttributes()) {
            attributeDao.create(attr);
        }

        getDao().create(entity);
    }
    
    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void delete(Table table) {
        getDao().delete(table);
        primaryKeyDao.delete(table.getPrimaryKey());
        lastModifiedKeyDao.delete(table.getLastModifiedKey());
    }
    
}
