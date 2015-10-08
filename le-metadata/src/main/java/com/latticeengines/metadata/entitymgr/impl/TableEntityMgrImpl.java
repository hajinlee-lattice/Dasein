package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import org.hibernate.Hibernate;
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
        setTenantId(entity);
        getDao().create(entity);
        updateReferences(entity);

        primaryKeyDao.create(entity.getPrimaryKey());
        lastModifiedKeyDao.create(entity.getLastModifiedKey());
        
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
    public void delete(String tableName) {
        Table found = findByName(tableName);
        if (found == null) {
            throw new RuntimeException("No table found with name " + tableName);
        }
        tableDao.delete(found);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Table> getAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Table> findAll() {
        List<Table> tables = super.findAll();
        return tables;
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Table findByName(String name) {
        Table table = tableDao.findByName(name);
        inflateTable(table);
        return table;
    }
    
    private void inflateTable(Table table) {
        if (table != null) {
            Hibernate.initialize(table.getAttributes());
            Hibernate.initialize(table.getExtracts());
            Hibernate.initialize(table.getPrimaryKey());
            Hibernate.initialize(table.getLastModifiedKey());
        }
    }

    private void setTenantId(Table table) {
        Tenant tenant = SecurityContextUtils.getTenant();

        // This is because Hibernate is horrible and produces two tenant ids
        if (tenant != null && tenant.getPid() != null && table.getTenantId() == null) {
            table.setTenant(tenant);
        }

        for (Extract extract : table.getExtracts()) {
            applyTenantIdToEntity(extract);
        }

        for (Attribute attr : table.getAttributes()) {
            applyTenantIdToEntity(attr);
        }
    }

    private void applyTenantIdToEntity(HasTenantId entity) {
        Tenant tenant = SecurityContextUtils.getTenant();

        if (tenant != null && tenant.getPid() != null && entity.getTenantId() == null) {
            entity.setTenantId(SecurityContextUtils.getTenant().getPid());
        }
    }

    private void updateReferences(Table table) {
        table.getPrimaryKey().setTable(table);
        table.getLastModifiedKey().setTable(table);
        for (Extract extract : table.getExtracts()) {
            extract.setTable(table);
        }

        for (Attribute attr : table.getAttributes()) {
            attr.setTable(table);
        }
    }
}
