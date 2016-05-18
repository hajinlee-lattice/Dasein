package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public interface ColumnMetadataInterface {
    List<ColumnMetadata> columnSelection(ColumnSelection.Predefined selectName);
    String selectionCurrentVersion(ColumnSelection.Predefined selectName);
}
