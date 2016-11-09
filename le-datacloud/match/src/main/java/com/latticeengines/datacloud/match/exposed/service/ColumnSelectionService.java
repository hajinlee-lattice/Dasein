package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public interface ColumnSelectionService extends HasDataCloudVersion {

    List<String> getMatchedColumns(ColumnSelection selection);

    ColumnSelection parsePredefinedColumnSelection(Predefined predefined);

    Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection);

    Map<String, Pair<BitCodeBook, List<String>>> getDecodeParameters(ColumnSelection columnSelection, String dataCloudVersion);

    String getCurrentVersion(Predefined predefined);
}
