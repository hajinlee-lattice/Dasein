package com.latticeengines.modelquality.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.ModelRun;

public interface ModelRunDao extends BaseDao<ModelRun> {

    List<ModelRun> findAllByAnalyticTest(AnalyticTest analyticTest);
}
