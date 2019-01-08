package com.latticeengines.dataplatform.service.impl.parallel;

import javax.annotation.Resource;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.modeling.ModelingJob;

@Component("awsBatchContainerDispatcher")
public class AwsBatchContainerDispatchImpl extends SingleContainerDispatchImpl {

    @Resource(name = "awsBatchModelingJobService")
    private ModelingJobService modelingJobService;

    @Override
    public ApplicationId submitJob(ModelingJob modelingJob, boolean isParallelEnabled, boolean isModeling) {
        return modelingJobService.submitJob(modelingJob);
    }

}
