package com.latticeengines.documentdb.entity;

import java.io.Serializable;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

public interface MetadataEntity extends Serializable {

    List<String> getnamespaceKeys();

    ColumnMetadata getMetadata();

}
