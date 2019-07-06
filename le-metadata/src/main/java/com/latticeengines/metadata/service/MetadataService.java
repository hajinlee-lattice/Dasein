package com.latticeengines.metadata.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.domain.Pageable;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeFixer;
import com.latticeengines.domain.exposed.metadata.StorageMechanism;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

public interface MetadataService {

    Table getTable(CustomerSpace parse, String tableName);

    Table getTable(CustomerSpace customerSpace, String name, Boolean includeAttributes);

    List<Table> getTables(CustomerSpace customerSpace);

    void createTable(CustomerSpace customerSpace, Table table);

    void deleteTableAndCleanup(CustomerSpace customerSpace, String name);

    void updateTable(CustomerSpace customerSpace, Table table);

    void renameTable(CustomerSpace customerSpace, String oldName, String newName);

    Map<String, Set<AnnotationValidationError>> validateTableMetadata(CustomerSpace customerSpace,
            ModelingMetadata modelingMetadata);

    List<Table> getImportTables(CustomerSpace customerSpace);

    void deleteImportTableAndCleanup(CustomerSpace customerSpace, String tableName);

    Table getImportTable(CustomerSpace customerSpace, String name);

    Table cloneTable(CustomerSpace customerSpace, String tableName, boolean ignoreExtracts);

    Table copyTable(CustomerSpace customerSpace, CustomerSpace targetCustomerSpace, String tableName);

    void setStorageMechanism(CustomerSpace customerSpace, String tableName, StorageMechanism storageMechanism);

    Boolean addAttributes(CustomerSpace space, String tableName, List<Attribute> attributes);

    Boolean fixAttributes(CustomerSpace space, String tableName, List<AttributeFixer> attributeFixerList);

    List<Attribute> getTableAttributes(CustomerSpace customerSpace, String tableName, Pageable pageable);

    Long getTableAttributeCount(CustomerSpace space, String tableName);

}
