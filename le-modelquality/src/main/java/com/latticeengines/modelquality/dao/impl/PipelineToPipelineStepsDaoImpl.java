package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.PipelineToPipelineSteps;
import com.latticeengines.modelquality.dao.PipelineToPipelineStepsDao;

@Component("pipelineToPipelineStepsDao")
public class PipelineToPipelineStepsDaoImpl extends ModelQualityBaseDaoImpl<PipelineToPipelineSteps>
        implements PipelineToPipelineStepsDao {

    @Override
    protected Class<PipelineToPipelineSteps> getEntityClass() {
        return PipelineToPipelineSteps.class;
    }

}
