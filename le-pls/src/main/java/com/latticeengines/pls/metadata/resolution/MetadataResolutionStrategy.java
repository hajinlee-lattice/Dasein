package com.latticeengines.pls.metadata.resolution;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Table;

public abstract class MetadataResolutionStrategy {

    public abstract void calculate();

    public abstract void calculateBasedOnExistingMetadata(Table metadataTable);

    public abstract List<ColumnTypeMapping> getUnknownColumns();

    public abstract boolean isMetadataFullyDefined();

    public abstract Table getMetadata();

}
