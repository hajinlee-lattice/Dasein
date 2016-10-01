package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.datacloud.match.service.HasDataCloudVersion;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public interface ColumnSelectionService extends HasDataCloudVersion {

    List<String> getMatchedColumns(ColumnSelection selection);

    ColumnSelection parsePredefinedColumnSelection(Predefined predefined);

    Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection);

    String getCurrentVersion(Predefined predefined);
}
