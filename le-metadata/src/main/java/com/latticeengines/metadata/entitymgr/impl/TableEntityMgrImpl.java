package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.dao.ExtractDao;
import com.latticeengines.metadata.dao.PrimaryKeyDao;
import com.latticeengines.metadata.dao.TableDao;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("tableEntityMgr")
public class TableEntityMgrImpl implements TableEntityMgr {
    
    @Autowired
    private AttributeDao attributeDao;
    
    @Autowired
    private ExtractDao extractDao;
    
    @Autowired
    private PrimaryKeyDao primaryKeyDao;

    @Autowired
    private TableDao tableDao;

    public BaseDao<Table> getDao() {
        return tableDao;
    }

    @Transactional(value = "mds", propagation = Propagation.REQUIRED)
    @Override
    public void create(Table entity) {
        primaryKeyDao.create(entity.getPrimaryKey());
        
        for (Extract extract : entity.getExtracts()) {
            extractDao.create(extract);
        }
        
        for (Attribute attr : entity.getAttributes()) {
            attributeDao.create(attr);
        }
        
        getDao().create(entity);
    }

    @Transactional(value = "mds", propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(Table entity) {
        getDao().createOrUpdate(entity);
    }

    @Transactional(value = "mds", propagation = Propagation.REQUIRED)
    @Override
    public void update(Table entity) {
        getDao().update(entity);
    }

    @Transactional(value = "mds", propagation = Propagation.REQUIRED)
    @Override
    public void delete(Table entity) {
        getDao().delete(entity);
    }

    @Transactional(value = "mds", propagation = Propagation.REQUIRED)
    @Override
    public void deleteAll() {
        getDao().deleteAll();
    }

    @Transactional(value = "mds", propagation = Propagation.REQUIRED)
    @Override
    public boolean containInSession(Table entity) {
        return getDao().containInSession(entity);
    }

    @Transactional(value = "mds", propagation = Propagation.REQUIRED)
    @Override
    public Table findByKey(Table entity) {
        return getDao().findByKey(entity);
    }

    @Transactional(value = "mds", propagation = Propagation.REQUIRED)
    @Override
    public List<Table> findAll() {
        return getDao().findAll();
    }
}
