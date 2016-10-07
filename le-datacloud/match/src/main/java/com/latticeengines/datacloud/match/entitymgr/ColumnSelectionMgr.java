package com.latticeengines.datacloud.match.entitymgr;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public interface ColumnSelectionMgr {

    ColumnSelection getPredefined(Predefined predefined, String dataCloudVersion);

    ColumnSelection getPredefinedAtVersion(Predefined predefined, String version);

    String getCurrentVersionOfPredefined(Predefined predefined);

}
