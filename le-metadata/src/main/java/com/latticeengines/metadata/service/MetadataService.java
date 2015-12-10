package com.latticeengines.metadata.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

public interface MetadataService {

    Table getTable(CustomerSpace customerSpace, String name);

    List<Table> getTables(CustomerSpace customerSpace);

    void createTable(CustomerSpace customerSpace, Table table);

    void deleteTable(CustomerSpace customerSpace, String name);

    void updateTable(CustomerSpace customerSpace, Table table);

    Map<String, Set<AnnotationValidationError>> validateTableMetadata(CustomerSpace customerSpace,
            ModelingMetadata modelingMetadata);

    List<Table> getImportTables(CustomerSpace customerSpace);

    void deleteImportTable(CustomerSpace customerSpace, String tableName);

    Table getImportTable(CustomerSpace customerSpace, String name);
}
