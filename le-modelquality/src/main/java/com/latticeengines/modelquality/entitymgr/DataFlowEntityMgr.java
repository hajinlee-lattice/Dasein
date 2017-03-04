package com.latticeengines.modelquality.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.DataFlow;

public interface DataFlowEntityMgr extends BaseEntityMgr<DataFlow> {

    void createDataFlows(List<DataFlow> dataflows);

    DataFlow findByName(String dataFlowName);

    DataFlow getLatestProductionVersion();
}
