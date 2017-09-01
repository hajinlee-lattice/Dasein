package com.latticeengines.network.exposed.propdata;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public interface ColumnMetadataInterface {
    List<ColumnMetadata> columnSelection(Predefined selectName, String dataCloudVersion);
    DataCloudVersion latestVersion(String compatibleVersion);
    Set<String> premiumAttributes(String dataCloudVersion);
}
