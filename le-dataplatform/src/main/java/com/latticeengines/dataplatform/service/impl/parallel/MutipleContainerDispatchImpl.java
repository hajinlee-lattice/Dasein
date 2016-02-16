package com.latticeengines.dataplatform.service.impl.parallel;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.runtime.python.PythonMRJobType;
import com.latticeengines.dataplatform.runtime.python.PythonMRProperty;
import com.latticeengines.dataplatform.service.ParallelDispatchService;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

@Component("mutipleContainerDispatcher")
public class MutipleContainerDispatchImpl implements ParallelDispatchService {

    @Value("${dataplatform.sampling.parallel.trainingset.number}")
    private String numberOfSamplingTrainingSet;

    @Value("${dataplatform.profiling.parallel.mapper.number}")
    private String numberOfProfilingMappers;

    @Resource(name = "parallelModelingJobService")
    private ModelingJobService modelingJobService;

    @Override
    public void customizeSampleConfig(SamplingConfiguration config, boolean isParallelEnabled) {

        config.setTrainingSetCount(Integer.parseInt(numberOfSamplingTrainingSet));
        config.setTrainingElements();
    }

    @Override
    public String getSampleJobName(boolean isParallelEnabled) {
        return "parallelSamplingJob";
    }

    @Override
    public String getModelingJobName(boolean isParallelEnabled) {
        return PythonMRJobType.MODELING_JOB.jobType();
    }

    @Override
    public String getNumberOfSamplingTrainingSet(boolean isParallelEnabled) {
        return numberOfSamplingTrainingSet;
    }
// TODO:
//    @Override
//    public long getSampleSize(Configuration yarnConfiguration, String diagnosticsPath, boolean isParallelEnabled)
//            throws Exception {
//        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, diagnosticsPath);
//        JsonNode diagnostics = new ObjectMapper().readTree(content);
//        long sampleSize = diagnostics.get("DataSummary").get("RowSize").asLong();
//
//        return sampleSize;
//    }
//    
    @Override
    public long getSampleSize(Configuration yarnConfiguration, String diagnosticsPath, boolean isParallelEnabled)
            throws Exception {
        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, diagnosticsPath);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(content);
        long sampleSize = (long) ((JSONObject) jsonObject.get("Summary")).get("SampleSize");
        return sampleSize;
    }

    @Override
    public String getTrainingFile(String samplePrefix, boolean isParallelEnabled) {
        return SamplingConfiguration.TRAINING_ALL_PREFIX;
    }

    @Override
    public String getTestFile(String samplePrefix, boolean isParallelEnabled) {
        return SamplingConfiguration.TESTING_SET_PREFIX;
    }

    @Override
    public String getNumberOfProfilingMappers(boolean isParallelEnabled) {
        return numberOfProfilingMappers;
    }

    @Override
    public String getProfileJobName(boolean isParallelEnabled) {
        return PythonMRJobType.PROFILING_JOB.jobType();
    }

    @Override
    public ApplicationId submitJob(ModelingJob modelingJob, boolean isParallelEnabled, boolean isModeling) {
        return modelingJobService.submitJob(modelingJob);
    }

    @Override
    public String getMapSizeKeyName(boolean isParallelEnabled) {
        return PythonMRProperty.MAPPER_SIZE.name();
    }

}
