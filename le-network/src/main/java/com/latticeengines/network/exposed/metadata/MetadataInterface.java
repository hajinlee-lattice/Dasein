package com.latticeengines.network.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

public interface MetadataInterface {

    Boolean resetTables(String customerSpace);

    Boolean createImportTable(String customerSpace, String tableName, Table table);

    List<String> getImportTableNames(String customerSpace);

    Table getImportTable(String customerSpace, String tableName);

    void deleteTable(String customerSpace, String tableName);

    void deleteImportTable(String customerSpace, String tableName);

    ModelingMetadata getTableMetadata(String customerSpace, String tableName);

    void updateImportTable(String customerSpace, String tableName, Table table);

    List<String> getTableNames(String customerSpace);

    Table getTable(String customerSpace, String tableName);

    void createTable(String customerSpace, String tableName, Table table);

    void updateTable(String customerSpace, String tableName, Table table);

    Table cloneTable(String customerSpace, String tableName);

    Table copyTable(String sourceTenantId, String targetCustomerSpace, String tableName);

    void validateArtifact(String customerSpace, ArtifactType artifactType, String filePath);

}
