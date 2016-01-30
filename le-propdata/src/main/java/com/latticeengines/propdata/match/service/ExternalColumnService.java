package com.latticeengines.propdata.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;

public interface ExternalColumnService {

    List<ExternalColumn> columnSelection(ColumnSelection.Predefined selectName);

}
