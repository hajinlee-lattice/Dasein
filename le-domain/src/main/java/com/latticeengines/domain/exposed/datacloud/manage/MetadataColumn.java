package com.latticeengines.domain.exposed.datacloud.manage;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

public interface MetadataColumn {

    ColumnMetadata toColumnMetadata();

    String getDisplayName();

    String getColumnId();

    boolean containsTag(String tag);
}
