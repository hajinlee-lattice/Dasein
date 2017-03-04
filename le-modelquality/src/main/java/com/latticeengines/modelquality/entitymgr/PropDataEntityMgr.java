package com.latticeengines.modelquality.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.PropData;

public interface PropDataEntityMgr extends BaseEntityMgr<PropData> {

    void createPropDatas(List<PropData> propDatas);

    PropData findByName(String propDataConfigName);

    PropData getLatestProductionVersion();

}
