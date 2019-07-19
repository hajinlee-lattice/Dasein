package com.latticeengines.metadata.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.persistence.RollbackException;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeFixer;
import com.latticeengines.domain.exposed.metadata.StorageMechanism;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.metadata.service.MetadataService;

@Component("mdService")
public class MetadataServiceImpl implements MetadataService {

    private static final Logger log = LoggerFactory.getLogger(MetadataServiceImpl.class);

    @Inject
    private TableEntityMgr tableEntityMgr;

    @Inject
    private TableTypeHolder tableTypeHolder;

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
        DatabaseUtils.retry("deleteTable", //
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
                    tableEntityMgr.deleteByName(found.getName());
                }

                tableEntityMgr.create(TableUtils.clone(table, table.getName()));
            });
        } finally {
            tableTypeHolder.setTableType(TableType.DATATABLE);
        }
    }

    @Override
    public void renameTable(CustomerSpace customerSpace, String oldName, String newName) {
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

}
