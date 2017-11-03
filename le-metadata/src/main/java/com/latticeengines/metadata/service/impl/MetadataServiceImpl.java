package com.latticeengines.metadata.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.Closure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private TableTypeHolder tableTypeHolder;

    @Override
    public Table getTable(CustomerSpace customerSpace, String name) {
        return tableEntityMgr.findByName(name);
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
    public void createTable(CustomerSpace customerSpace, final Table table) {
        DatabaseUtils.retry("createTable", new Closure() {
            @Override
            public void execute(Object input) {
                tableEntityMgr.create(TableUtils.clone(table, table.getName()));
            }
        });
    }

    @Override
    public void deleteTableAndCleanup(CustomerSpace customerSpace, final String tableName) {
        DatabaseUtils.retry("deleteTable", new Closure() {
            @Override
            public void execute(Object input) {
                tableEntityMgr.deleteTableAndCleanupByName(tableName);
            }
        });
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
            DatabaseUtils.retry("updateTable", new Closure() {
                @Override
                public void execute(Object input) {
                    Table found = tableEntityMgr.findByName(table.getName());
                    if (found != null) {
                        log.info(String.format("Table %s already exists.  Deleting first.", table.getName()));
                        tableEntityMgr.deleteByName(found.getName());
                    }

                    tableEntityMgr.create(TableUtils.clone(table, table.getName()));
                }
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
    public Table cloneTable(CustomerSpace customerSpace, String tableName) {
        return tableEntityMgr.clone(tableName);
    }

    @Override
    public Table copyTable(CustomerSpace customerSpace, CustomerSpace targetCustomerSpace, String tableName) {
        return tableEntityMgr.copy(tableName, targetCustomerSpace);
    }

    @Override
    public void setStorageMechanism(CustomerSpace customerSpace, final String tableName,
            StorageMechanism storageMechanism) {
        tableTypeHolder.setTableType(TableType.DATATABLE);
        DatabaseUtils.retry("addStorageMechanism", new Closure() {
            @Override
            public void execute(Object input) {
                Table found = tableEntityMgr.findByName(tableName);
                found.setStorageMechanism(storageMechanism);
                updateTable(customerSpace, found);
            }
        });
    }
}
