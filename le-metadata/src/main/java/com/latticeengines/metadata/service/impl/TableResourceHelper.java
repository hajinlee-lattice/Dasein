package com.latticeengines.metadata.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeFixer;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.metadata.service.MetadataService;

@Component("tableResourceHelper")
public class TableResourceHelper {

    private static final Logger log = LoggerFactory.getLogger(TableResourceHelper.class);

    @Inject
    private MetadataService mdService;

    public List<String> getTables(String customerSpace) {
        log.info(String.format("getTables(%s)", customerSpace));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        List<Table> tables = mdService.getTables(space);
        List<String> tableNames = new ArrayList<>();
        for (Table table : tables) {
            tableNames.add(table.getName());
        }
        return tableNames;
    }

    public Table getTable(String customerSpace, String tableName, Boolean includeAttributes) {
        log.info(String.format("getTable(%s, %s, %s)", customerSpace, tableName, includeAttributes));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return mdService.getTable(space, tableName, includeAttributes);
    }

    public Long getTableAttributeCount(String customerSpace, String tableName) {
        log.info(String.format("getTableAttributeCount(%s, %s)", customerSpace, tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return mdService.getTableAttributeCount(space, tableName);
    }

    public List<Attribute> getTableAttributes(String customerSpace, String tableName, Pageable pageable) {
        log.info(String.format("getTableAttributes(%s, %s, %s)", customerSpace, tableName, pageable));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return mdService.getTableAttributes(space, tableName, pageable);
    }
    
    public ModelingMetadata getTableMetadata(String customerSpace, String tableName) {
        log.info(String.format("getTableMetadata(%s, %s)", customerSpace, tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        Table table = mdService.getTable(space, tableName, true);
        return table.getModelingMetadata();
    }

    public Boolean createTable(String customerSpace, //
            String tableName, //
            Table table) {
        log.info(String.format("createTable(%s), with Attributes: %d", table.getName(), table.getAttributes().size()));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        mdService.createTable(space, table);
        return true;
    }

    public Boolean updateTable(String customerSpace, //
            String tableName, //
            Table table) {
        log.info(String.format("updateTable(%s), with Attributes: %d", table.getName(), table.getAttributes().size()));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        if (!tableName.equals(table.getName())) {
            log.info(String.format("renameTable (Deprecated. Instead use renameTable) from (%s), to : (%s)", table.getName(), tableName));
            mdService.renameTable(space, table.getName(), tableName);
            table.setName(tableName);
        } else {
            mdService.updateTable(space, table);
        }
        return true;
    }
    
    public Boolean renameTable(String customerSpace, //
            String tableName, //
            String newTableName) {
        log.info(String.format("renameTable from (%s), to : (%s)", tableName, newTableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        mdService.renameTable(space, tableName, newTableName);
        return true;
    }

    public Boolean deleteTableAndCleanup(String customerSpace, //
            String tableName) {
        log.info(String.format("deleteTableAndCleanup(%s)", tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        mdService.deleteTableAndCleanup(space, tableName);
        return true;
    }

    public Boolean deleteImportTableAndCleanup(String customerSpace, //
            String tableName) {
        log.info(String.format("deleteImportTableAndCleanup(%s)", tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        mdService.deleteImportTableAndCleanup(space, tableName);
        return true;
    }

    public Table cloneTable(String customerSpace, //
            String tableName) {
        log.info(String.format("cloneTable(%s)", tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return mdService.cloneTable(space, tableName);
    }

    public Table copyTable(String customerSpace, //
            String targetCustomerSpace, String tableName) {
        log.info(String.format("copyTable(%s(", tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return mdService.copyTable(space, CustomerSpace.parse(targetCustomerSpace), tableName);
    }

    public SimpleBooleanResponse validateMetadata(String customerSpace, ModelingMetadata metadata) {
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        Map<String, Set<AnnotationValidationError>> validationErrors = mdService.validateTableMetadata(space, metadata);
        SimpleBooleanResponse response = SimpleBooleanResponse.successResponse();
        if (validationErrors.size() > 0) {
            List<String> errors = new ArrayList<>();
            for (Map.Entry<String, Set<AnnotationValidationError>> entry : validationErrors.entrySet()) {

                for (AnnotationValidationError error : entry.getValue()) {
                    errors.add(
                            String.format("Error with field %s for column %s.", error.getFieldName(), entry.getKey()));
                }

            }
            response = SimpleBooleanResponse.failedResponse(errors);
        }
        return response;
    }

    public Boolean resetTables(String customerSpace) {
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        List<Table> tables = mdService.getTables(space);
        for (Table table : tables) {
            mdService.deleteTableAndCleanup(space, table.getName());
        }
        List<Table> importTables = mdService.getImportTables(space);
        for (Table table : importTables) {
            table = mdService.getImportTable(space, table.getName());
            table.setPid(null);

            LastModifiedKey lmk = table.getLastModifiedKey();
            lmk.setPid(null);
            DateTime date = new DateTime();
            lmk.setLastModifiedTimestamp(date.minusYears(2).getMillis());
            table.getPrimaryKey().setPid(null);
            table.setExtracts(Collections.<Extract> emptyList());

            List<Attribute> attrs = table.getAttributes();
            for (Attribute attr : attrs) {
                attr.setPid(null);
            }
            mdService.updateTable(space, table);
        }
        return true;
    }

    public Boolean createTableAttributes(String customerSpace, String tableName, List<Attribute> attributes) {
        log.info(String.format("createTableAttributes(%s, %s, %d)", customerSpace, tableName, (attributes!=null ? attributes.size() : 0)));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return mdService.addAttributes(space, tableName, attributes);
    }

    public Boolean fixTableAttributes(String customerSpace, String tableName, List<AttributeFixer> attributeFixerList) {
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return mdService.fixAttributes(space, tableName, attributeFixerList);
    }

}
