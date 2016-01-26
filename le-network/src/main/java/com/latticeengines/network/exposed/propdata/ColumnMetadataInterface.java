package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;

public interface ColumnMetadataInterface {
    List<ColumnMetadata> columnSelection(String selectName);
}
