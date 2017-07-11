package com.latticeengines.metadata.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.StorageMechanism;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.metadata.service.MetadataService;

@Component("tableResourceHelper")
public class TableResourceHelper {

    private static final Log log = LogFactory.getLog(TableResourceHelper.class);

    @Autowired
    private MetadataService mdService;

    public List<String> getTables(String customerSpace, HttpServletRequest request) {
        log.info(String.format("getTables(%s)", customerSpace));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        List<Table> tables = mdService.getTables(space);
        List<String> tableNames = new ArrayList<>();
        for (Table table : tables) {
            tableNames.add(table.getName());
        }
        return tableNames;
    }

    public Table getTable(String customerSpace, String tableName, HttpServletRequest request) {
        log.info(String.format("getTable(%s, %s)", customerSpace, tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return mdService.getTable(space, tableName);
    }

    public ModelingMetadata getTableMetadata(String customerSpace, String tableName, HttpServletRequest request) {
        log.info(String.format("getTableMetadata(%s, %s)", customerSpace, tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        Table table = mdService.getTable(space, tableName);
        return table.getModelingMetadata();
    }

    public Boolean createTable(String customerSpace, //
            String tableName, //
            Table table, //
            HttpServletRequest request) {
        log.info(String.format("createTable(%s)", table.getName()));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        mdService.createTable(space, table);
        return true;
    }

    public Boolean updateTable(String customerSpace, //
            String tableName, //
            Table table, //
            HttpServletRequest request) {
        log.info(String.format("updateTable(%s)", table.getName()));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        if (!tableName.equals(table.getName())) {
            mdService.renameTable(space, table.getName(), tableName);
            table.setName(tableName);
        } else {
            mdService.updateTable(space, table);
        }
        return true;
    }

    public Boolean deleteTableAndCleanup(String customerSpace, //
            String tableName, //
            HttpServletRequest request) {
        log.info(String.format("deleteTableAndCleanup(%s)", tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        mdService.deleteTableAndCleanup(space, tableName);
        return true;
    }

    public Boolean deleteImportTableAndCleanup(String customerSpace, //
            String tableName, //
            HttpServletRequest request) {
        log.info(String.format("deleteImportTableAndCleanup(%s)", tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        mdService.deleteImportTableAndCleanup(space, tableName);
        return true;
    }

    public Table cloneTable(String customerSpace, //
            String tableName, HttpServletRequest request) {
        log.info(String.format("cloneTable(%s(", tableName));
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return mdService.cloneTable(space, tableName);
    }

    public Table copyTable(String customerSpace, //
            String targetCustomerSpace, String tableName, HttpServletRequest request) {
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

    public Boolean resetTables(String customerSpace, HttpServletRequest request) {
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

    public void setStorageMechanism(String customerSpace, String tableName, StorageMechanism storageMechanism) {
        mdService.setStorageMechanism(CustomerSpace.parse(customerSpace), tableName, storageMechanism);
    }

}
