package com.latticeengines.propdata.match.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.Predefined;

public interface ColumnSelectionService {

    boolean accept(String version);

    List<String> getMatchedColumns(ColumnSelection selection);

    ColumnSelection parsePredefinedColumnSelection(Predefined predefined);

    Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection);

    String getCurrentVersion(Predefined predefined);

    Boolean isValidVersion(Predefined predefined, String version);
}
