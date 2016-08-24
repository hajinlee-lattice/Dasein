package com.latticeengines.propdata.match.entitymanager;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.Predefined;

public interface ColumnSelectionMgr {

    ColumnSelection getPredefined(Predefined predefined);

    ColumnSelection getPredefinedAtVersion(Predefined predefined, String version);

    String getCurrentVersionOfPredefined(Predefined predefined);

}
