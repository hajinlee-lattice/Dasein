package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;
import java.util.UUID;

import org.apache.commons.collections.Closure;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.hibernate.Hibernate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.dao.ExtractDao;
import com.latticeengines.metadata.dao.LastModifiedKeyDao;
import com.latticeengines.metadata.dao.PrimaryKeyDao;
import com.latticeengines.metadata.dao.TableDao;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.hive.HiveTableDao;
import com.latticeengines.metadata.service.impl.MetadataServiceImpl;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("tableEntityMgr")
public class TableEntityMgrImpl implements TableEntityMgr {

    private static final Logger log = Logger.getLogger(MetadataServiceImpl.class);

    @Value("${metadata.hive.enabled:false}")
    private boolean hiveEnabled;

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

    @Autowired
    private HiveTableDao hiveTableDao;

    private Configuration yarnConfiguration = new Configuration();

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(Table entity) {
        setTenantId(entity);
        tableDao.create(entity);
        updateReferences(entity);

        if (entity.getPrimaryKey() != null) {
            primaryKeyDao.create(entity.getPrimaryKey());
        }
        if (entity.getLastModifiedKey() != null) {
            lastModifiedKeyDao.create(entity.getLastModifiedKey());
        }

        if (entity.getExtracts() != null) {
            for (Extract extract : entity.getExtracts()) {
                extractDao.create(extract);
            }
        }

        if (entity.getAttributes() != null) {
            for (Attribute attr : entity.getAttributes()) {
                attributeDao.create(attr);
            }
        }

        if (hiveEnabled) {
            hiveTableDao.create(entity);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void deleteByName(String name) {
        final Table entity = findByName(name);
        if (entity != null) {
            tableDao.delete(entity);
            if (hiveEnabled) {
                hiveTableDao.deleteIfExists(entity);
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Table> findAll() {
        List<Table> tables = tableDao.findAll();
        return tables;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Table findByName(String name) {
        Table table = tableDao.findByName(name);
        inflateTable(table);
        return table;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Table clone(String name) {
        Table existing = findByName(name);
        if (existing == null) {
            throw new RuntimeException(String.format("No such table with name %s", name));
        }

        final Table clone = TableUtils.clone(existing);
        clone.setName(UUID.randomUUID().toString() + "_clone");

        DatabaseUtils.retry("createTable", new Closure() {
            @Override
            public void execute(Object input) {
                create(TableUtils.clone(clone));
            }
        });

        if (clone.getExtracts().size() > 0) {
            Path tablesPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                    MultiTenantContext.getCustomerSpace());

            Path sourcePath = new Path(ExtractUtils.getSingleExtractPath(yarnConfiguration, clone));
            Path destPath = tablesPath.append(clone.getName());
            log.info(String.format("Copying table data from %s to %s", sourcePath, destPath));
            try {
                HdfsUtils.copyFiles(yarnConfiguration, sourcePath.toString(), destPath.toString());
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to copy in HDFS from %s to %s", sourcePath.toString(),
                        destPath.toString()), e);
            }
        }
        return clone;
    }

    private static void inflateTable(Table table) {
        if (table != null) {
            Hibernate.initialize(table.getAttributes());
            Hibernate.initialize(table.getExtracts());
            Hibernate.initialize(table.getPrimaryKey());
            Hibernate.initialize(table.getLastModifiedKey());
        }
    }

    private void setTenantId(Table table) {
        Tenant tenant = MultiTenantContext.getTenant();

        // This is because Hibernate is horrible and produces two tenant ids
        if (tenant != null && tenant.getPid() != null && table.getTenantId() == null) {
            table.setTenant(tenant);
        }

        if (table.getExtracts() != null) {
            for (Extract extract : table.getExtracts()) {
                applyTenantIdToEntity(extract);
            }
        }

        if (table.getAttributes() != null) {
            for (Attribute attr : table.getAttributes()) {
                applyTenantIdToEntity(attr);
            }
        }
    }

    private void applyTenantIdToEntity(HasTenantId entity) {
        Tenant tenant = MultiTenantContext.getTenant();

        if (tenant != null && tenant.getPid() != null && entity.getTenantId() == null) {
            entity.setTenantId(MultiTenantContext.getTenant().getPid());
        }
    }

    private void updateReferences(Table table) {
        if (table.getPrimaryKey() != null) {
            table.getPrimaryKey().setTable(table);
        }
        if (table.getLastModifiedKey() != null) {
            table.getLastModifiedKey().setTable(table);
        }

        if (table.getExtracts() != null) {
            for (Extract extract : table.getExtracts()) {
                extract.setTable(table);
            }
        }

        if (table.getAttributes() != null) {
            for (Attribute attr : table.getAttributes()) {
                attr.setTable(table);
            }
        }
    }
}
