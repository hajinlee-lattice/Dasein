package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.Predefined;

public interface ColumnMetadataInterface {
    List<ColumnMetadata> columnSelection(Predefined selectName, String dataCloudVersion);
    String selectionCurrentVersion(Predefined selectName);
}
