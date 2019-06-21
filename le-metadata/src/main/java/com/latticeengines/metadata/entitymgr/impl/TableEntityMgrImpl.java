package com.latticeengines.metadata.entitymgr.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.ImmutableList;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeFixer;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.RedshiftDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.dao.DataRuleDao;
import com.latticeengines.metadata.dao.ExtractDao;
import com.latticeengines.metadata.dao.LastModifiedKeyDao;
import com.latticeengines.metadata.dao.PrimaryKeyDao;
import com.latticeengines.metadata.dao.TableDao;
import com.latticeengines.metadata.entitymgr.DataUnitEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.hive.HiveTableDao;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

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

    @Inject
    private DataUnitEntityMgr dataUnitEntityMgr;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
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
            int batchSize = attributeDao.getBatchSize();
            int counter = 0;
            for (Attribute attr : entity.getAttributes()) {
                attributeDao.create(attr);
                counter++;
                if (counter % batchSize == 0) {
                    attributeDao.flushSession();
                    attributeDao.clearSession();
                }
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
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void addAttributes(String name, List<Attribute> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return;
        }
        Table existingTable = findByName(name, false);
        if (existingTable == null) {
            throw new RuntimeException(String.format("No such table with name %d", name));
        }

        Long tenantId = existingTable.getTenantId();
        for (Attribute attr : attributes) {
            attr.setTable(existingTable);
            attr.setTenantId(tenantId);
            attributeDao.create(attr);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void addExtract(Table table, Extract extract) {
        table.addExtract(extract);
        extractDao.create(extract);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void deleteByName(String name) {
        final Table entity = findByName(name, false);
        if (entity != null) {
            tableDao.delete(entity);
            if (hiveEnabled) {
                hiveTableDao.deleteIfExists(entity);
            }
        }
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void deleteTableAndCleanupByName(String name) {
        final Table entity = findByName(name);
        if (entity != null) {
            List<String> extractPaths = ExtractUtils.getExtractPaths(yarnConfiguration, entity);
            deleteByName(name);
            deleteExtractsInBackend(extractPaths);
            deleteExternalStorage(name);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Table> findAll() {
        List<Table> tables = tableDao.findAll();
        return tables;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Table findByName(String name) {
        return findByName(name, true);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Table findByName(String name, boolean inflate) {
        return findByName(name, true, true);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Table findByName(String name, boolean inflate, boolean includeAttributes) {
        Table table = tableDao.findByName(name);
        if (inflate) {
            TableEntityMgr.inflateTable(table, includeAttributes);
        }
        return table;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public Table clone(String name) {
        Table existing = findByName(name);
        if (existing == null) {
            throw new RuntimeException(String.format("No such table with name %s", name));
        }

        final Table clone = TableUtils.clone(existing, "clone_" + UUID.randomUUID().toString().replace('-', '_'));

        String cloneTable = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                MultiTenantContext.getCustomerSpace(), existing.getNamespace()).append(clone.getName()).toString();
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, cloneTable)
                    || HdfsUtils.fileExists(yarnConfiguration, cloneTable)) {
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
            for (int i = 0; i < existing.getExtracts().size(); i++) {
                Extract extract = existing.getExtracts().get(i);
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
                    // Multiple extracts could contain files with same name. We
                    // need to rename to avoid copy failure due to name conflict
                    String renameSuffix = existing.getExtracts().size() > 1 ? "_" + String.valueOf(i) : null;
                    if (singleFile) {
                        srcPath = new HdfsToS3PathBuilder(useEmr).getS3PathWithGlob(yarnConfiguration, srcPath, false,
                                s3Bucket);
                        log.info(String.format("Copying table data from %s to %s", srcPath, cloneTable));
                        HdfsUtils.copyFiles(yarnConfiguration, srcPath, cloneTable);
                        if (existing.getExtracts().size() > 1) {
                            String fileName = new org.apache.hadoop.fs.Path(srcPath).getName();
                            String rename = HdfsUtils.appendSuffixToFileName(fileName, renameSuffix);
                            HdfsUtils.rename(yarnConfiguration,
                                    new org.apache.hadoop.fs.Path(cloneTable, fileName).toString(),
                                    new org.apache.hadoop.fs.Path(cloneTable, rename).toString());
                            log.info(String.format(
                                    "Rename file %s to %s to avoid potential name conflict for files in multiple extracts",
                                    new org.apache.hadoop.fs.Path(cloneTable, fileName).toString(),
                                    new org.apache.hadoop.fs.Path(cloneTable, rename).toString()));
                        }
                    } else {
                        srcPath = new HdfsToS3PathBuilder(useEmr).getS3Dir(yarnConfiguration, srcPath, s3Bucket);
                        log.info(String.format("Copying table data as glob from %s to %s", srcPath, cloneTable));
                        HdfsUtils.copyGlobToDirWithScheme(yarnConfiguration, srcPath, cloneTable, renameSuffix);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Failed to copy in HDFS from %s to %s", srcPath, cloneTable), e);
                }

                if (extract.getProcessedRecords() != null && extract.getProcessedRecords() > 0) {
                    count.addAndGet(extract.getProcessedRecords());
                }
            }
        }
        newExtract.setProcessedRecords(count.get());
        newExtract.setExtractionTimestamp(System.currentTimeMillis());
        clone.setExtracts(Collections.singletonList(newExtract));

        String oldTableSchema = PathBuilder.buildDataTableSchemaPath(CamilleEnvironment.getPodId(),
                MultiTenantContext.getCustomerSpace(), existing.getNamespace()).append(name).toString();
        String cloneTableSchema = oldTableSchema.replace(name, clone.getName());
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, oldTableSchema)) {
                HdfsUtils.copyFiles(yarnConfiguration, oldTableSchema, cloneTableSchema);
                log.info(String.format("Copying table schema from %s to %s", oldTableSchema, cloneTableSchema));
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to copy schema in HDFS from %s to %s", oldTableSchema, cloneTableSchema), e);
        }

        DatabaseUtils.retry("createTable", input -> create(TableUtils.clone(clone, clone.getName())));

        return clone;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public Table copy(String name, final CustomerSpace targetCustomerSpace) {
        Table existing = findByName(name);
        if (existing == null) {
            throw new RuntimeException(String.format("No such table with name %s", name));
        }

        final Table copy = TableUtils.clone(existing, "copy_" + UUID.randomUUID().toString().replace('-', '_'));

        DatabaseUtils.retry("createTable", input -> {
            Tenant t = tenantEntityMgr.findByTenantId(targetCustomerSpace.toString());
            MultiTenantContext.setTenant(t);
            copy.setTenant(t);
            create(TableUtils.clone(copy, copy.getName()));
        });

        if (copy.getExtracts().size() > 0) {
            Path tablesPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), targetCustomerSpace,
                    existing.getNamespace());

            String sourcePath = ExtractUtils.getSingleExtractPath(yarnConfiguration, copy, true, s3Bucket, useEmr);
            String destPath = tablesPath + "/" + copy.getName();
            try {
                log.info(String.format("Copying table data from %s to %s", sourcePath, destPath));
                HdfsUtils.mkdir(yarnConfiguration, destPath);
                HdfsUtils.copyFiles(yarnConfiguration, sourcePath, destPath);
                String s3DestPath = new HdfsToS3PathBuilder(useEmr).exploreS3FilePath(destPath, s3Bucket);
                log.info(String.format("Copying table data from %s to %s", sourcePath, s3DestPath));
                HdfsUtils.mkdir(yarnConfiguration, s3DestPath);
                HdfsUtils.copyFiles(yarnConfiguration, sourcePath, s3DestPath);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to copy in HDFS from %s to %s", sourcePath.toString(),
                        destPath.toString()), e);
            }
        }
        return copy;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public Table rename(String oldName, String newName) {
        Table existing = findByName(oldName, false);
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

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Long countAttributesByTable_Pid(Long tablePid) {
        return attributeDao.countByTablePid(tablePid);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Attribute> findAttributesByTable_Pid(Long tablePid, Pageable pageable) {
        return attributeDao.findByTablePid(tablePid, pageable);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void fixAttributes(String name, List<AttributeFixer> attributeFixerList) {
        Table table = tableDao.findByName(name);
        TableEntityMgr.inflateTable(table, true);
        for (AttributeFixer attributeFixer : attributeFixerList) {
            Attribute attribute = table.getAttribute(attributeFixer.getName());
            if (attribute != null) {
                attributeFixer.copyToAttribute(attribute);
                attributeDao.update(attribute);
            }
        }
    }

    private void deleteExtractsInBackend(List<String> extractPaths) {
        final ImmutableList<String> finalPaths = ImmutableList.copyOf(extractPaths);
        new Thread(() -> finalPaths.forEach(p -> {
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
        })).start();
    }

    private void deleteExternalStorage(String tableName) {
        try {
            List<DataUnit> dataUnits = dataUnitEntityMgr.deleteAllByName(tableName);
            log.info("Deleted " + dataUnits.size() + " data units associated with table " + tableName);
            for (DataUnit unit : dataUnits) {
                if (DataUnit.StorageType.Redshift.equals(unit.getStorageType())) {
                    RedshiftDataUnit redshiftDataUnit = (RedshiftDataUnit) unit;
                    String redshiftTable = redshiftDataUnit.getRedshiftTable();
                    try {
                        redshiftService.dropTable(redshiftTable);
                    } catch (Exception e) {
                        log.error(String.format("Failed to drop table %s from redshift", redshiftTable), e);
                    }
                } else if (DataUnit.StorageType.S3.equals(unit.getStorageType())) {
                    S3DataUnit s3DataUnit = (S3DataUnit) unit;
                    String s3Url = s3DataUnit.getLinkedDir();
                    Matcher matcher = Pattern.compile("^(s3a|s3n|s3)://(?<bucket>[^/]+)" //
                            + "/(?<prefix>.*)").matcher(s3Url);
                    if (matcher.matches()) {
                        String bucket = matcher.group("bucket");
                        String prefix = matcher.group("prefix");
                        if (prefix.endsWith(".avro")) {
                            prefix = prefix.substring(0, prefix.lastIndexOf("/"));
                        }
                        s3Service.cleanupPrefix(bucket, prefix);
                    } else {
                        log.warn("s3 data unit " + unit.getName() +" has an invalid url " + s3Url);
                    }
                }
            }
        } catch (Exception e) {
            log.error(String.format("Failed to clean up data unit by table name %s", tableName), e);
        }
        try {
            redshiftService.dropTable(AvroUtils.getAvroFriendlyString(tableName));
        } catch (Exception e) {
            log.error(
                    String.format("Failed to drop table %s from redshift", AvroUtils.getAvroFriendlyString(tableName)),
                    e);
        }
    }

}
