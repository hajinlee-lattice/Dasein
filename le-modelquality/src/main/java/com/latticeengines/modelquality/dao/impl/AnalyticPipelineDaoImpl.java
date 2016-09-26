package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.modelquality.dao.AnalyticPipelineDao;

@Component("qualityAnalyticPipelineDao")
public class AnalyticPipelineDaoImpl extends BaseDaoImpl<AnalyticPipeline> implements AnalyticPipelineDao {

    @Override
    protected Class<AnalyticPipeline> getEntityClass() {
        return AnalyticPipeline.class;
    }

}
