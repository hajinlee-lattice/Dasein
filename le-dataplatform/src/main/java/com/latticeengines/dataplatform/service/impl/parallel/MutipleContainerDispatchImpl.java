package com.latticeengines.dataplatform.service.impl.parallel;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.runtime.python.PythonMRJobType;
import com.latticeengines.dataplatform.runtime.python.PythonMRProperty;
import com.latticeengines.dataplatform.service.ParallelDispatchService;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

@Component("MutipleContainerDispatcher")
public class MutipleContainerDispatchImpl implements ParallelDispatchService {

    @Value("${dataplatform.sampling.trainingset.number}")
    private String numberOfSamplingTrainingSet;

    @Value("${dataplatform.profiling.mapper.number}")
    private String numberOfProfilingMappers;

    @Resource(name = "parallelModelingJobService")
    private ModelingJobService modelingJobService;

    @Override
    public void customizeSampleConfig(SamplingConfiguration config) {

        config.setTrainingSetCount(Integer.parseInt(numberOfSamplingTrainingSet));
        config.setTrainingElements();
    }

    @Override
    public String getSampleJobName() {
        return "parallelSamplingJob";
    }

    @Override
    public String getModelingJobName() {
        return PythonMRJobType.MODELING_JOB.jobType();
    }

    @Override
    public String getNumberOfSamplingTrainingSet() {
        return numberOfSamplingTrainingSet;
    }

    @Override
    public long getSampleSize(Configuration yarnConfiguration, String diagnosticsPath) throws Exception {
        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, diagnosticsPath);
        JsonNode diagnostics = new ObjectMapper().readTree(content);
        long sampleSize = diagnostics.get("DataSummary").get("RowSize").asLong();

        return sampleSize;
    }

    @Override
    public String getTrainingFile(String samplePrefix) {
        return SamplingConfiguration.TRAINING_ALL_PREFIX;
    }

    @Override
    public String getTestFile(String samplePrefix) {
        return SamplingConfiguration.TESTING_SET_PREFIX;
    }

    @Override
    public String getNumberOfProfilingMappers() {
        return numberOfProfilingMappers;
    }

    @Override
    public String getProfileJobName() {
        return PythonMRJobType.PROFILING_JOB.jobType();
    }

    @Override
    public ApplicationId submitJob(ModelingJob modelingJob) {
        return modelingJobService.submitJob(modelingJob);
    }

    @Override
    public Object getMapSizeKeyName() {
        return PythonMRProperty.MAPPER_SIZE.name();
    }

}
