package com.latticeengines.propdata.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public interface ColumnMetadataService {
    List<ColumnMetadata> fromPredefinedSelection (ColumnSelection.Predefined selectionName);
}
