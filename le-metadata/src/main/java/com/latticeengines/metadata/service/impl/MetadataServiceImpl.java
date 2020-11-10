package com.latticeengines.metadata.service.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.persistence.RollbackException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeFixer;
import com.latticeengines.domain.exposed.metadata.StorageMechanism;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.PrestoDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyUpdateDetail;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.metadata.annotation.NoCustomSpaceAndType;
import com.latticeengines.metadata.entitymgr.MigrationTrackEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.metadata.service.DataUnitService;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.prestodb.exposed.service.AthenaService;
import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;
import com.latticeengines.prestodb.exposed.service.PrestoDbService;

@Component("mdService")
public class MetadataServiceImpl implements MetadataService {

    private static final Logger log = LoggerFactory.getLogger(MetadataServiceImpl.class);

    @Inject
    private TableEntityMgr tableEntityMgr;

    @Inject
    private TableTypeHolder tableTypeHolder;

    @Inject
    private MigrationTrackEntityMgr migrationTrackEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private PrestoConnectionService prestoConnectionService;

    @Inject
    private PrestoDbService prestoDbService;

    @Inject
    private AthenaService athenaService;

    @Inject
    private DataUnitService dataUnitService;

    @Override
    public Table getTable(CustomerSpace customerSpace, String name) {
        return getTable(customerSpace, name, true);
    }

    @Override
    public Table getTable(CustomerSpace customerSpace, String name, Boolean includeAttributes) {
        Table table = tableEntityMgr.findByName(name, true, includeAttributes);
        if (table != null && !includeAttributes) {
            table.setAttributes(Collections.emptyList());
        }
        return table;
    }

    @Override
    public List<Table> getTables(CustomerSpace customerSpace) {
        return tableEntityMgr.findAll();
    }

    @Override
    public Table getImportTable(CustomerSpace customerSpace, String name) {
        tableTypeHolder.setTableType(TableType.IMPORTTABLE);
        try {
            return tableEntityMgr.findByName(name);
        } finally {
            tableTypeHolder.setTableType(TableType.DATATABLE);
        }
    }

    @Override
    public List<Table> getImportTables(CustomerSpace customerSpace) {
        tableTypeHolder.setTableType(TableType.IMPORTTABLE);
        try {
            return tableEntityMgr.findAll();
        } finally {
            tableTypeHolder.setTableType(TableType.DATATABLE);
        }
    }

    @Override
    public Long getTableAttributeCount(CustomerSpace customerSpace, String tableName) {
        Long tablePid = getPidByTableName(customerSpace, tableName);
        if (tablePid == null) {
            return 0L;
        }
        Long count = tableEntityMgr.countAttributesByTable_Pid(tablePid);
        return count;
    }

    private Long getPidByTableName(CustomerSpace customerSpace, String tableName) {
        Table table = tableEntityMgr.findByName(tableName, false);
        /*
         * // For backward compatibility, we cannot throw the exception. So, returning null. if (table == null) throw
         * new LedpException(LedpCode.LEDP_11008, new String[] {tableName, customerSpace.toString()});
         */
        return table != null ? table.getPid() : null;
    }

    @Override
    public List<Attribute> getTableAttributes(CustomerSpace customerSpace, String tableName, Pageable pageable) {
        Long tablePid = getPidByTableName(customerSpace, tableName);
        List<Attribute> attributes = null;
        if (tablePid != null) {
            attributes = tableEntityMgr.findAttributesByTable_Pid(tablePid, pageable);
        }
        if (attributes == null) {
            return Collections.emptyList();
        }
        return attributes;
    }

    @Override
    public void createTable(CustomerSpace customerSpace, final Table table) {
        DatabaseUtils.retry("createTable", //
                input -> tableEntityMgr.create(TableUtils.clone(table, table.getName())));
    }

    @Override
    public void deleteTableAndCleanup(CustomerSpace customerSpace, final String tableName) {
        if (!migrationTrackEntityMgr.canDeleteOrRenameTable(tenantEntityMgr.findByTenantId(customerSpace.toString()), tableName)) {
            log.error("Tenant {} is in migration. Deleting active table is not allowed.", customerSpace.toString());
            throw new IllegalStateException(String.format("Tenant %s is in migration.", customerSpace.toString()));
        }
        DatabaseUtils.retry("deleteTable", 10, RollbackException.class, "RollbackException  detected performing",
                null, //
                input -> tableEntityMgr.deleteTableAndCleanupByName(tableName));
    }

    @Override
    public void deleteImportTableAndCleanup(CustomerSpace customerSpace, String tableName) {
        tableTypeHolder.setTableType(TableType.IMPORTTABLE);
        try {
            deleteTableAndCleanup(customerSpace, tableName);
        } finally {
            tableTypeHolder.setTableType(TableType.DATATABLE);
        }
    }

    @Override
    public void updateTable(CustomerSpace customerSpace, final Table table) {
        tableTypeHolder.setTableType(table.getTableType());
        try {
            log.info("Metadata UpdateTable: {}", table.getName());
            DatabaseUtils.retry("updateTable", 10, RollbackException.class, "RollbackException  detected performing", null,
                    input -> {
                        Table found = tableEntityMgr.findByName(table.getName(), false);
                        if (found != null) {
                            log.info(String.format("Table %s already exists.  Deleting first.", table.getName()));
                            if (!migrationTrackEntityMgr.canDeleteOrRenameTable(found.getTenant(), found.getName())) {
                                log.error("Tenant {} is in migration. Deleting active table is not allowed.", customerSpace.toString());
                                throw new IllegalStateException(String.format("Tenant %s is in migration.", customerSpace.toString()));
                            }
                            tableEntityMgr.deleteByName(found.getName());
                        }
                        Table tableToClone = TableUtils.clone(table, table.getName());
                        if (found != null) {
                            tableToClone.setRetentionPolicy(found.getRetentionPolicy());
                        }
                        tableEntityMgr.create(tableToClone);
                    });
        } finally {
            tableTypeHolder.setTableType(TableType.DATATABLE);
        }
    }

    @Override
    public void renameTable(CustomerSpace customerSpace, String oldName, String newName) {
        if (!migrationTrackEntityMgr.canDeleteOrRenameTable(tenantEntityMgr.findByTenantId(customerSpace.toString()), oldName)) {
            log.error("Tenant {} is in migration. Renaming tables is not allowed.", customerSpace.toString());
            throw new IllegalStateException(String.format("Tenant %s is in migration.", customerSpace.toString()));
        }
        tableEntityMgr.rename(oldName, newName);
    }

    @Override
    public Map<String, Set<AnnotationValidationError>> validateTableMetadata(CustomerSpace customerSpace,
                                                                             ModelingMetadata modelingMetadata) {
        BeanValidationService validationService = new BeanValidationServiceImpl();
        try {
            Map<String, Set<AnnotationValidationError>> errors = new HashMap<>();
            for (AttributeMetadata m : modelingMetadata.getAttributeMetadata()) {
                Set<AnnotationValidationError> attributeErrors = validationService.validate(m);

                if (attributeErrors.size() > 0) {
                    errors.put(m.getColumnName(), attributeErrors);
                }
            }
            return errors;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    @Override
    public Table cloneTable(CustomerSpace customerSpace, String tableName, boolean ignoreExtracts) {
        return tableEntityMgr.clone(tableName, ignoreExtracts);
    }

    @Override
    public Table copyTable(CustomerSpace customerSpace, CustomerSpace targetCustomerSpace, String tableName) {
        return tableEntityMgr.copy(tableName, targetCustomerSpace);
    }

    @Override
    public void setStorageMechanism(CustomerSpace customerSpace, final String tableName,
                                    StorageMechanism storageMechanism) {
        tableTypeHolder.setTableType(TableType.DATATABLE);
        DatabaseUtils.retry("addStorageMechanism", input -> {
            Table found = tableEntityMgr.findByName(tableName);
            found.setStorageMechanism(storageMechanism);
            updateTable(customerSpace, found);
        });
    }

    @Override
    public Boolean addAttributes(CustomerSpace space, String tableName, List<Attribute> attributes) {
        tableEntityMgr.addAttributes(tableName, attributes);
        return true;
    }

    @Override
    public Boolean fixAttributes(CustomerSpace space, String tableName, List<AttributeFixer> attributeFixerList) {
        if (CollectionUtils.isEmpty(attributeFixerList)) {
            return true;
        }
        tableEntityMgr.fixAttributes(tableName, attributeFixerList);
        return true;
    }

    @Override
    public void updateTableRetentionPolicy(CustomerSpace customerSpace, String tableName, RetentionPolicy retentionPolicy) {
        tableEntityMgr.updateTableRetentionPolicy(tableName, retentionPolicy);
    }

    @Override
    public void updateTableRetentionPolicies(CustomerSpace customerSpace, RetentionPolicyUpdateDetail retentionPolicyUpdateDetail) {
        tableEntityMgr.updateTableRetentionPolicies(retentionPolicyUpdateDetail);
    }

    @Override
    public void updateImportTableUpdatedBy(CustomerSpace customerSpace, Table table) {
        tableTypeHolder.setTableType(TableType.IMPORTTABLE);
        try {
            tableEntityMgr.updateUpdatedBy(table);
        } finally {
            tableTypeHolder.setTableType(TableType.DATATABLE);
        }
    }

    @NoCustomSpaceAndType
    @Override
    public List<Table> findAllWithExpiredRetentionPolicy(int index, int max) {
        return tableEntityMgr.findAllWithExpiredRetentionPolicy(index, max);
    }

    @Override
    public PrestoDataUnit registerPrestoDataUnit(CustomerSpace customerSpace, String tableName) {
        if (TableType.DATATABLE.equals(tableTypeHolder.getTableType())) {
            String clusterId = prestoConnectionService.getClusterId();
            PrestoDataUnit oldDataUnit = (PrestoDataUnit) //
                    dataUnitService.findByNameTypeFromReader(tableName, DataUnit.StorageType.Presto);
            if (oldDataUnit != null) {
                Set<String> clusterIds = oldDataUnit.getPrestoTableNames().keySet();
                log.info("Already found a presto data unit named {} in clusters {}", tableName, clusterIds);
                if (clusterIds.contains(clusterId) && //
                        prestoDbService.tableExists(oldDataUnit.getPrestoTableName(clusterId))) {
                    log.info("No need to register presto data unit named {} in cluster {}", tableName, clusterId);
                    return oldDataUnit;
                }
            }

            Table table = getTable(customerSpace, tableName);
            Preconditions.checkNotNull(table, //
                    "Cannot find table named " + tableName + " in " + customerSpace.getTenantId());
            HdfsDataUnit hdfsDataUnit = table.toHdfsDataUnit(tableName);
            String path = hdfsDataUnit.getPath();
            Preconditions.checkArgument(StringUtils.isNotBlank(path), //
                    "Table " + tableName + " does not have a hdfs path.");
            boolean fileExists;
            String dir = PathUtils.toParquetOrAvroDir(path);
            try {
                fileExists = HdfsUtils.fileExists(yarnConfiguration, dir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to check if path exists in hdfs: " + dir);
            }
            if (!fileExists) {
                throw new IllegalStateException("Hdfs path " + path + " does not exists in cluster " + clusterId + ".");
            }
            log.info("Going to register presto data unit using hdfs path {}", path);
            hdfsDataUnit.setTenant(customerSpace.getTenantId());
            PrestoDataUnit newDataUnit = prestoDbService.saveDataUnit(hdfsDataUnit);
            if (oldDataUnit != null) {
                log.info("Register an existing presto data unit named {} in cluster {}", tableName, clusterId);
                oldDataUnit.addPrestoTableName(clusterId, newDataUnit.getPrestoTableName(clusterId));
                return (PrestoDataUnit) dataUnitService.createOrUpdateByNameAndStorageType(oldDataUnit);
            } else {
                log.info("Register a new presto data unit named {} in cluster {}", tableName, clusterId);
                return (PrestoDataUnit) dataUnitService.createOrUpdateByNameAndStorageType(newDataUnit);
            }
        } else {
            throw new IllegalStateException("Can only register data table to presto");
        }
    }

    @Override
    public AthenaDataUnit registerAthenaDataUnit(CustomerSpace customerSpace, String tableName) {
        if (TableType.DATATABLE.equals(tableTypeHolder.getTableType())) {
            // find athena data unit
            AthenaDataUnit oldDataUnit = (AthenaDataUnit) //
                    dataUnitService.findByNameTypeFromReader(tableName, DataUnit.StorageType.Athena);
            if (oldDataUnit != null && athenaService.tableExists(oldDataUnit.getAthenaTable())) {
                log.info("Already found a athena data unit named {} : {}", tableName, oldDataUnit.getAthenaTable());
                return oldDataUnit;
            }
            // find s3 data unit
            S3DataUnit s3DataUnit = (S3DataUnit) //
                    dataUnitService.findByNameTypeFromReader(tableName, DataUnit.StorageType.S3);
            Preconditions.checkNotNull(s3DataUnit, "Cannot find s3 data unit named " + tableName);
            AthenaDataUnit athenaDataUnit = athenaService.saveDataUnit(s3DataUnit);
            return (AthenaDataUnit) dataUnitService.createOrUpdateByNameAndStorageType(athenaDataUnit);
        } else {
            throw new IllegalStateException("Can only register data table to presto");
        }
    }
}
