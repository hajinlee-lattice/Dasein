package com.latticeengines.metadata.exposed.resolution;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.exposed.standardschemas.SchemaRepository;

public class UserDefinedMetadataResolutionStrategy extends MetadataResolutionStrategy {
    private String csvPath;
    private SchemaInterpretation schema;
    private List<ColumnTypeMapping> additionalColumns;

    // TODO Take in a list of column-type mappings for columns that aren't part
    // of the standard schema. Then this will:
    // - throw an exception if some columns not specified
    // - throw an exception if there's no header
    // - remove columns from metadata that don't exist
    // - if required columns don't exist, throw an exception

    // Interaction with front end:
    // - FE -> BE: uploadFile
    // - FE -> BE: getUnknownColumns(SourceFile)
    // - FE -> BE: resolveMetadata(SourceFile, unknowncolumns)
    public UserDefinedMetadataResolutionStrategy(String csvPath, SchemaInterpretation schema,
                                                 List<ColumnTypeMapping> additionalColumns) {
        this.csvPath = csvPath;
        this.schema = schema;
        this.additionalColumns = additionalColumns != null ? additionalColumns : new ArrayList<ColumnTypeMapping>();
    }

    @Override
    public List<ColumnTypeMapping> getUnknownColumns() {
        return new ArrayList<>();
    }

    @Override
    public boolean isMetadataFullyDefined() {
        return true;
    }

    @Override
    public Table getMetadata() {
        SchemaRepository repository = new SchemaRepository();
        Table metadata = repository.getSchema(schema);
        return metadata;
    }
}
