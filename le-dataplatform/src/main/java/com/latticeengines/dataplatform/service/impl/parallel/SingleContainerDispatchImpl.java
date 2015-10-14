package com.latticeengines.dataplatform.service.impl.parallel;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.runtime.python.PythonMRProperty;
import com.latticeengines.dataplatform.service.ParallelDispatchService;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

@Component("SingleContainerDispatcher")
public class SingleContainerDispatchImpl implements ParallelDispatchService {

    @Resource(name = "modelingJobService")
    private ModelingJobService modelingJobService;

    @Override
    public void customizeSampleConfig(SamplingConfiguration config) {
    }

    @Override
    public String getSampleJobName() {
        return "samplingJob";
    }

    @Override
    public String getModelingJobName() {
        return "modeling";
    }

    @Override
    public String getNumberOfSamplingTrainingSet() {
        return "0";
    }

    @Override
    public long getSampleSize(Configuration yarnConfiguration, String diagnosticsPath) throws Exception {
        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, diagnosticsPath);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(content);
        long sampleSize = (long) ((JSONObject) jsonObject.get("Summary")).get("SampleSize");
        return sampleSize;
    }

    @Override
    public String getTrainingFile(String samplePrefix) {
        return samplePrefix + "Training";
    }

    @Override
    public String getTestFile(String samplePrefix) {
        return samplePrefix + "Test";
    }

    @Override
    public String getNumberOfProfilingMappers() {
        return "0";
    }

    @Override
    public String getProfileJobName() {
        return "profiling";
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
