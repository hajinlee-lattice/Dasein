package com.latticeengines.documentdb.entity;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.IsColumnMetadata;

public interface ColumnMetadataDocument<T extends IsColumnMetadata> extends NameSpacedDocument<T> {

    default ColumnMetadata getColumnMetadata() {
        return getDocument().toColumnMetadata();
    }

}
