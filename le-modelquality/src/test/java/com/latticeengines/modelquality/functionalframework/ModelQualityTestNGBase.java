package com.latticeengines.modelquality.functionalframework;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.modelquality.entitymgr.AlgorithmEntityMgr;
import com.latticeengines.modelquality.entitymgr.AnalyticPipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.AnalyticTestEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataFlowEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.entitymgr.ModelConfigEntityMgr;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineStepEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineToPipelineStepsEntityMgr;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-modelquality-context.xml" })
public class ModelQualityTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${modelquality.file.upload.hdfs.dir}")
    protected String hdfsDir;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected AlgorithmEntityMgr algorithmEntityMgr;
    @Autowired
    protected DataFlowEntityMgr dataFlowEntityMgr;
    @Autowired
    protected DataSetEntityMgr dataSetEntityMgr;
    @Autowired
    protected PipelineEntityMgr pipelineEntityMgr;
    @Autowired
    protected PipelineStepEntityMgr pipelineStepEntityMgr;
    @Autowired
    protected PipelineToPipelineStepsEntityMgr pipelineToPipelineStepsEntityMgr;
    @Autowired
    protected PropDataEntityMgr propDataEntityMgr;
    @Autowired
    protected ModelRunEntityMgr modelRunEntityMgr;
    @Autowired
    protected ModelConfigEntityMgr modelConfigEntityMgr;
    @Autowired
    protected SamplingEntityMgr samplingEntityMgr;
    @Autowired
    protected AnalyticPipelineEntityMgr analyticPipelineEntityMgr;
    @Autowired
    protected AnalyticTestEntityMgr analyticTestEntityMgr;

    protected void cleanupHdfs() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, hdfsDir + "/steps");
        HdfsUtils.rmdir(yarnConfiguration, hdfsDir + "/pipelines");
    }

    protected void cleanupDb() {
        modelRunEntityMgr.deleteAll();
        modelConfigEntityMgr.deleteAll();
        analyticTestEntityMgr.deleteAll();
        analyticPipelineEntityMgr.deleteAll();
        algorithmEntityMgr.deleteAll();
        dataFlowEntityMgr.deleteAll();
        dataSetEntityMgr.deleteAll();
        pipelineToPipelineStepsEntityMgr.deleteAll();
        pipelineStepEntityMgr.deleteAll();
        pipelineEntityMgr.deleteAll();
        propDataEntityMgr.deleteAll();
        samplingEntityMgr.deleteAll();
    }

}
