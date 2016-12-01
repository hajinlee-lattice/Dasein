package com.latticeengines.modelquality.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;

public interface AnalyticTestDao extends BaseDao<AnalyticTest> {
    List<AnalyticTest> findAllByAnalyticPipeline(AnalyticPipeline ap);
}
