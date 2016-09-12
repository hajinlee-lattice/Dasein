package com.latticeengines.datacloud.match.entitymgr;

import com.latticeengines.domain.exposed.datacloud.manage.ColumnSelection;
import com.latticeengines.domain.exposed.datacloud.manage.ColumnSelection.Predefined;

public interface ColumnSelectionMgr {

    ColumnSelection getPredefined(Predefined predefined);

    ColumnSelection getPredefinedAtVersion(Predefined predefined, String version);

    String getCurrentVersionOfPredefined(Predefined predefined);

}
