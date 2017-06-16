package com.latticeengines.serviceflows.workflow.etl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.client.YarnClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

public abstract class BaseTransformationStep<T extends BaseStepConfiguration> extends BaseWorkflowStep<T> {

    private static int MAX_LOOPS = 1800;
    private static final ObjectMapper OM = new ObjectMapper();
    private static Log log = LogFactory.getLog(BaseTransformationStep.class);

    @Autowired
    protected TransformationProxy transformationProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    protected YarnClient yarnClient;

    @Value("${pls.cdl.transform.workflow.mem.mb}")
    protected int workflowMemMb;

    @Value("${pls.cdl.transform.workflow.mem.mb.max}")
    protected int workflowMemMbMax;

    @Value("${pls.cdl.transform.cascading.partitions}")
    private int cascadingPartitions;

    @Value("${pls.cdl.transform.tez.task.mem.gb}")
    private int tezMemGb;

    protected String getDataCloudVersion() {
        return columnMetadataProxy.latestVersion("").getVersion();
    }

    protected String appendEngineConf(TransformerConfig conf, TransformationFlowParameters.EngineConfiguration engineConf) {
        ObjectNode on = OM.valueToTree(conf);
        on.set("EngineConfig", OM.valueToTree(engineConf));
        return JsonUtils.serialize(on);
    }

    protected void waitForFinish(TransformationProgress progress) {
        TransformationProgress progressInDb = null;
        String appIdStr = progress.getYarnAppId();
        ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);
        YarnUtils.waitFinalStatusForAppId(yarnClient, appId);
        for (int i = 0; i < MAX_LOOPS; i++) {
            progressInDb = transformationProxy.getProgress(progress.getRootOperationUID());
            if (ProgressStatus.FINISHED.equals(progressInDb.getStatus())
                    || ProgressStatus.FAILED.equals(progressInDb.getStatus())) {
                break;
            }
            if (i % 3 == 0) {
                log.info("TransformationProgress Id=" + progressInDb.getPid() + " status=" + progressInDb.getStatus());
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                log.warn("Waiting was interrupted, message=" + e.getMessage());
            }
        }

        if (ProgressStatus.FINISHED.equals(progressInDb.getStatus())) {
            log.info("Consolidate data pipeline is " + ProgressStatus.FINISHED);
        } else if (ProgressStatus.FAILED.equals(progressInDb.getStatus())) {
            String error = "Consolidate data pipeline failed!";
            log.error(error);
            throw new RuntimeException(error + " Error=" + progressInDb.getErrorMessage());
        } else {
            String error = "Consolidate data pipeline timeout!";
            log.error(error);
            throw new RuntimeException(error);
        }
    }

    protected String emptyStepConfig() {
        return appendEngineConf(new TransformerConfig(), baseEngineConfig());
    }

    protected TransformationFlowParameters.EngineConfiguration baseEngineConfig() {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine("TEZ");
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 1024));
        jobProperties.put("mapreduce.job.reduces", String.valueOf(cascadingPartitions));
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions);
        return engineConf;
    }

    protected TransformationFlowParameters.EngineConfiguration localFlinkEngineConfig() {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine("FLINK");
        engineConf.setPartitions(cascadingPartitions);
        return engineConf;
    }
}
