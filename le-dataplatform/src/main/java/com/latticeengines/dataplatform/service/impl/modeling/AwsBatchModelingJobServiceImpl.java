package com.latticeengines.dataplatform.service.impl.modeling;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modeling.ModelingJob;

@Component("awsBatchModelingJobService")
public class AwsBatchModelingJobServiceImpl extends ModelingJobServiceImpl {


    protected ApplicationId submitJobInternal(ModelingJob modelingJob) {

        return super.submitAwsBatchJob(modelingJob);
    }


}
