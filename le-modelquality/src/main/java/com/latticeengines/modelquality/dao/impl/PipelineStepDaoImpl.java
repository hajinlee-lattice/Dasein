package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.modelquality.dao.PipelineStepDao;

@Component("pipelineStepDao")
public class PipelineStepDaoImpl extends ModelQualityBaseDaoImpl<PipelineStep> implements PipelineStepDao {

    @Override
    protected Class<PipelineStep> getEntityClass() {
        return PipelineStep.class;
    }

}
