package com.latticeengines.domain.exposed.propdata.manage;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

public interface MetadataColumn {

    ColumnMetadata toColumnMetadata();
    String getDisplayName();
    String getColumnId();
}
