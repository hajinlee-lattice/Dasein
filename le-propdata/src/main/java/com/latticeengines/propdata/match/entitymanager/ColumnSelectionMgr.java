package com.latticeengines.propdata.match.entitymanager;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public interface ColumnSelectionMgr {

    ColumnSelection getPredefined(ColumnSelection.Predefined predefined);

    ColumnSelection getPredefinedAtVersion(ColumnSelection.Predefined predefined, String version);

    String getCurrentVersionOfPredefined(ColumnSelection.Predefined predefined);

}
