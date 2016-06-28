package com.latticeengines.modelquality.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.DataSet;

public interface DataSetEntityMgr extends BaseEntityMgr<DataSet> {

    void createDataSets(List<DataSet> datasets);

}
