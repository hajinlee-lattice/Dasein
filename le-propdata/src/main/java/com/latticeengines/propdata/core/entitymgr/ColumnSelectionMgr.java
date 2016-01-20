package com.latticeengines.propdata.core.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;

public interface ColumnSelectionMgr {

    ColumnSelection getPredefined(ColumnSelection.Predefined predefined);

    ColumnSelection getPredefinedAtVersion(ColumnSelection.Predefined predefined, String version);

    String getCurrentVersionOfPredefined(ColumnSelection.Predefined predefined);

    List<ExternalColumn> toExternalColumns(ColumnSelection selection);

}
