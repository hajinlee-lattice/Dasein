package com.latticeengines.dataplatform.service.impl.parallel;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.service.DispatchService;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.modeling.EventCounterConfiguration;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.yarn.exposed.runtime.python.PythonMRProperty;

@Component("singleContainerDispatcher")
public class SingleContainerDispatchImpl implements DispatchService {

    @Resource(name = "modelingJobService")
    private ModelingJobService modelingJobService;

    @Override
    public void customizeSampleConfig(SamplingConfiguration config, boolean isParallelEnabled) {
    }

    @Override
    public void customizeEventCounterConfig(EventCounterConfiguration config, boolean isParallelEnabled) {
    }

    @Override
    public String getSampleJobName(boolean isParallelEnabled) {
        return "samplingJob";
    }

    @Override
    public String getEventCounterJobName(boolean isParallelEnabled) {
        return "eventCounterJob";
    }

    @Override
    public String getModelingJobName(boolean isParallelEnabled) {
        return "modeling";
    }

    @Override
    public String getNumberOfSamplingTrainingSet(boolean isParallelEnabled) {
        return "0";
    }

    @Override
    public long getSampleSize(Configuration yarnConfiguration, String diagnosticsPath, boolean isParallelEnabled)
            throws Exception {
        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, diagnosticsPath);
        ObjectMapper jsonParser = new ObjectMapper();
        JsonNode jsonObject = jsonParser.readTree(content);
        long sampleSize = jsonObject.get("Summary").get("SampleSize").asLong();
        return sampleSize;
    }

    @Override
    public String getTrainingFile(String samplePrefix, boolean isParallelEnabled) {
        return samplePrefix + "Training";
    }

    @Override
    public String getTestFile(String samplePrefix, boolean isParallelEnabled) {
        return samplePrefix + "Test";
    }

    @Override
    public String getNumberOfProfilingMappers(boolean isParallelEnabled) {
        return "0";
    }

    @Override
    public String getProfileJobName(boolean isParallelEnabled) {
        return "profiling";
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
