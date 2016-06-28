package com.latticeengines.modelquality.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.Pipeline;

public interface PipelineEntityMgr extends BaseEntityMgr<Pipeline> {

    void deletePipelines(List<Pipeline> pipelines);

    void createPipelines(List<Pipeline> pipelines);

}
