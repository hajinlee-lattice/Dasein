package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.dao.ExtractDao;
import com.latticeengines.metadata.dao.LastModifiedKeyDao;
import com.latticeengines.metadata.dao.PrimaryKeyDao;
import com.latticeengines.metadata.dao.TableDao;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.hive.HiveTableDao;
import com.latticeengines.metadata.service.impl.MetadataServiceImpl;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import com.mysql.jdbc.StringUtils;

@Component("tableEntityMgr")
public class TableEntityMgrImpl implements TableEntityMgr {

    @SuppressWarnings("unused")
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
    public void create(final Table entity) {
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

        Table clone = JsonUtils.clone(existing);

        Pattern pattern = Pattern.compile("^(.*)_clone([0-9]*)$");
        Matcher matcher = pattern.matcher(clone.getName());
        if (matcher.matches()) {
            String counterString = matcher.group(2);
            int counter;
            if (StringUtils.isNullOrEmpty(counterString)) {
                counter = 1;
            } else {
                counter = Integer.parseInt(counterString);
            }
            counter++;
            clone.setName(matcher.group(1) + "_clone" + counter);
        } else {
            clone.setName(existing.getName() + "_clone");
        }

        create(clone);

        if (clone.getExtracts().size() > 0) {
            Path tablesPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                    SecurityContextUtils.getCustomerSpace());

            Path sourcePath = tablesPath.append(name);
            Path destPath = tablesPath.append(existing.getName());
            log.info(String.format("Copying table data from %s to %s", sourcePath, destPath));
            try {
                HdfsUtils.copyFiles(yarnConfiguration, sourcePath.toString(), destPath.toString());
            } catch (Exception e) {
                // throw new
                // RuntimeException(String.format("Failed to copy in HDFS from %s to %s",
                // sourcePath.toString(),
                // destPath.toString()), e);
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
        Tenant tenant = SecurityContextUtils.getTenant();

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
        Tenant tenant = SecurityContextUtils.getTenant();

        if (tenant != null && tenant.getPid() != null && entity.getTenantId() == null) {
            entity.setTenantId(SecurityContextUtils.getTenant().getPid());
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
