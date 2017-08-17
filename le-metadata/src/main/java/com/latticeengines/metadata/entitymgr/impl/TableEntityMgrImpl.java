package com.latticeengines.metadata.entitymgr.impl;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.Closure;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.dao.DataRuleDao;
import com.latticeengines.metadata.dao.ExtractDao;
import com.latticeengines.metadata.dao.LastModifiedKeyDao;
import com.latticeengines.metadata.dao.PrimaryKeyDao;
import com.latticeengines.metadata.dao.TableDao;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.hive.HiveTableDao;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("tableEntityMgr")
public class TableEntityMgrImpl implements TableEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(TableEntityMgrImpl.class);

    @Value("${metadata.hive.enabled:false}")
    private boolean hiveEnabled;

    @Autowired
    private AttributeDao attributeDao;

    @Autowired
    private DataRuleDao dataRuleDao;

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

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private RedshiftService redshiftService;

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

        if (entity.getDataRules() != null) {
            for (DataRule dataRule : entity.getDataRules()) {
                dataRuleDao.create(dataRule);
            }
        }

        if (hiveEnabled) {
            hiveTableDao.create(entity);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void addExtract(Table table, Extract extract) {
        table.addExtract(extract);
        extractDao.create(extract);
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

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void deleteTableAndCleanupByName(String name) {
        final Table entity = findByName(name);
        if (entity != null) {
            List<String> extractPaths = ExtractUtils.getExtractPaths(yarnConfiguration, entity);
            deleteByName(name);
            extractPaths.forEach(p -> {
                String avroDir = p.substring(0, p.lastIndexOf("/"));
                try {
                    HdfsUtils.rmdir(yarnConfiguration, avroDir);
                } catch (IOException e) {
                    log.error(String.format("Failed to delete extract %s", avroDir), e);
                }
                String schemaPath = avroDir.replace("/Tables", "/TableSchemas");
                try {
                    if (HdfsUtils.fileExists(yarnConfiguration, schemaPath)) {
                        HdfsUtils.rmdir(yarnConfiguration, schemaPath);
                    }
                } catch (IOException e) {
                    log.error(String.format("Failed to delete extract schema %s", schemaPath), e);
                }
            });
            try {
                redshiftService.dropTable(AvroUtils.getAvroFriendlyString(name));
            } catch (Exception e) {
                log.error(String.format("Failed to drop table %s from redshift", AvroUtils.getAvroFriendlyString(name)), e);
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
        TableEntityMgr.inflateTable(table);
        return table;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Table clone(String name) {
        Table existing = findByName(name);
        if (existing == null) {
            throw new RuntimeException(String.format("No such table with name %s", name));
        }

        final Table clone = TableUtils.clone(existing, "clone_" + UUID.randomUUID().toString().replace('-', '_'));

        DatabaseUtils.retry("createTable", input -> create(TableUtils.clone(clone, clone.getName())));


        String cloneTable = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                MultiTenantContext.getCustomerSpace(), existing.getNamespace()).append(clone.getName()).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, cloneTable)) {
                HdfsUtils.rmdir(yarnConfiguration, cloneTable);
            }
            HdfsUtils.mkdir(yarnConfiguration, cloneTable);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create table dir at " + cloneTable);
        }
        Extract newExtract = new Extract();
        newExtract.setPath(cloneTable + "/*.avro");
        newExtract.setName(NamingUtils.uuid("Extract"));
        AtomicLong count = new AtomicLong(0);
        if (existing.getExtracts() != null && existing.getExtracts().size() > 0) {
            existing.getExtracts().forEach(extract -> {
                String srcPath = extract.getPath();
                boolean singleFile = false;
                if (!srcPath.endsWith("*.avro")) {
                    if (srcPath.endsWith(".avro")) {
                        singleFile = true;
                    } else {
                        srcPath = srcPath.endsWith("/") ? srcPath : srcPath + "/";
                        srcPath += "*.avro";
                    }
                }
                try {
                    if (singleFile) {
                        log.info(String.format("Copying table data from %s to %s", srcPath, cloneTable));
                        HdfsUtils.copyFiles(yarnConfiguration, srcPath, cloneTable);
                    } else {
                        log.info(String.format("Copying table data as glob from %s to %s", srcPath, cloneTable));
                        HdfsUtils.copyGlobToDir(yarnConfiguration, srcPath, cloneTable);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Failed to copy in HDFS from %s to %s", srcPath,
                            cloneTable), e);
                }

                if (extract.getProcessedRecords() != null && extract.getProcessedRecords() > 0) {
                    count.addAndGet(extract.getProcessedRecords());
                }
            });
        }
        newExtract.setProcessedRecords(count.get());

        String oldTableSchema = PathBuilder.buildDataTableSchemaPath(CamilleEnvironment.getPodId(),
                MultiTenantContext.getCustomerSpace(), existing.getNamespace()).append(name).toString();
        String cloneTableSchema = oldTableSchema.replace(name, clone.getName());
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, oldTableSchema)) {
                HdfsUtils.copyFiles(yarnConfiguration, oldTableSchema, cloneTableSchema);
                log.info(String.format("Copying table schema from %s to %s", oldTableSchema, cloneTableSchema));
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to copy schema in HDFS from %s to %s", oldTableSchema,
                    cloneTableSchema), e);
        }

        return clone;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Table copy(String name, final CustomerSpace targetCustomerSpace) {
        Table existing = findByName(name);
        if (existing == null) {
            throw new RuntimeException(String.format("No such table with name %s", name));
        }

        final Table copy = TableUtils.clone(existing, "copy_" + UUID.randomUUID().toString().replace('-', '_'));

        DatabaseUtils.retry("createTable", new Closure() {
            @Override
            public void execute(Object input) {
                Tenant t = tenantEntityMgr.findByTenantId(targetCustomerSpace.toString());
                MultiTenantContext.setTenant(t);
                copy.setTenant(t);
                create(TableUtils.clone(copy, copy.getName()));
            }
        });

        if (copy.getExtracts().size() > 0) {
            Path tablesPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), targetCustomerSpace,
                    existing.getNamespace());

            Path sourcePath = new Path(ExtractUtils.getSingleExtractPath(yarnConfiguration, copy));
            Path destPath = tablesPath.append(copy.getName());
            log.info(String.format("Copying table data from %s to %s", sourcePath, destPath));
            try {
                HdfsUtils.copyFiles(yarnConfiguration, sourcePath.toString(), destPath.toString());
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to copy in HDFS from %s to %s", sourcePath.toString(),
                        destPath.toString()), e);
            }
        }
        return copy;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Table rename(String oldName, String newName) {
        Table existing = findByName(oldName);
        if (existing == null) {
            throw new RuntimeException(String.format("No such table with name %s", oldName));
        }
        existing.setName(newName);
        tableDao.update(existing);
        return existing;
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

        if (table.getDataRules() != null) {
            for (DataRule dataRule : table.getDataRules()) {
                dataRule.setTable(table);
            }
        }
    }
}
