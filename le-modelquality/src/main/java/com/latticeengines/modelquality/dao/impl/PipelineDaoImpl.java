package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.modelquality.dao.PipelineDao;

@Component("pipelineDao")
public class PipelineDaoImpl extends BaseDaoImpl<Pipeline> implements PipelineDao {

    @Override
    protected Class<Pipeline> getEntityClass() {
        return Pipeline.class;
    }

}
