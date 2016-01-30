package com.latticeengines.propdata.match.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public interface ColumnSelectionService {

    List<ColumnMetadata> getMetaData(ColumnSelection selection);

    Map<String, List<String>> getSourceColumnMap(ColumnSelection.Predefined predefined);
}
