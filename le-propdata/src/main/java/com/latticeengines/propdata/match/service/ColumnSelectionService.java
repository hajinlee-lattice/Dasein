package com.latticeengines.propdata.match.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public interface ColumnSelectionService {

    List<String> getMatchedColumns(ColumnSelection selection);

    ColumnSelection parsePredefined(ColumnSelection.Predefined predefined);

    Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection);

    String getCurrentVersion(ColumnSelection.Predefined predefined);

    Boolean isValidVersion(ColumnSelection.Predefined predefined, String version);
}
