package com.latticeengines.serviceflows.workflow.dataflow;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkScriptStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.LocalFileSparkScript;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.SparkScript;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class RunSparkScript <S extends SparkScriptStepConfiguration> extends BaseWorkflowStep<S> {

    @Inject
    private SparkJobService sparkJobService;

    @Inject
    private LivySessionHolder livySessionHolder;

    @Value("${camille.zk.pod.id}")
    private String podId;

    protected CustomerSpace customerSpace;
    protected boolean skipScriptExecution;

    protected abstract String getScriptPath();

    protected abstract List<DataUnit> getInputUnits();

    protected abstract void postScriptExecution(SparkJobResult result);

    @Override
    public void execute() {
        String scriptPath = getScriptPath();
        try {
            if (StringUtils.isBlank(scriptPath) || !HdfsUtils.fileExists(yarnConfiguration, scriptPath)) {
                throw new IOException("Invalid script path " + scriptPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to find spark script at path " + scriptPath);
        }
        log.info("Executing spark script " + scriptPath);
        customerSpace = CustomerSpace.parse(getConfiguration().getCustomer());

        String fileName = downloadScript(scriptPath);
        SparkScript script = toSparkScript(fileName);

        preScriptExecution();

        if (!skipScriptExecution) {
            ScriptJobConfig scriptConfig = getScriptConfig();
            scriptConfig.setInput(getInputUnits());

            String tenantId = customerSpace.getTenantId();
            String workspace = PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString();
            scriptConfig.setWorkspace(workspace);

            log.info("Spark script configuration: " + JsonUtils.serialize(scriptConfig));
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            SparkJobResult result = retry.execute(context -> {
                if (context.getRetryCount() > 0) {
                    log.warn("Previous failure:", context.getLastThrowable());
                    log.info("Attempt=" + (context.getRetryCount() + 1) + ": retry running spark script " //
                            + scriptPath);
                }
                LivySession session = livySessionHolder //
                        .createLivySession(tenantId + "~" + fileName);
                return sparkJobService.runScript(session, script, scriptConfig);
            });
            postScriptExecution(result);
            try {
                HdfsUtils.rmdir(yarnConfiguration, workspace);
            } catch (Exception e) {
                log.warn("Failed to clean up workspace", e);
            }
            livySessionHolder.killSession();
        }
    }

    protected void preScriptExecution() {
    }

    private String downloadScript(String path) {
        String fileName = path.substring(path.lastIndexOf("/") + 1);
        try {
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, fileName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to download spark script", e);
        }
        return fileName;
    }

    private SparkScript toSparkScript(String fileName) {
        LocalFileSparkScript sparkScript = new LocalFileSparkScript();
        sparkScript.setFile(new File(fileName));
        SparkInterpreter interpreter;
        if (fileName.endsWith(".scala")) {
            interpreter = SparkInterpreter.Scala;
        } else if (fileName.endsWith(".py")) {
            interpreter = SparkInterpreter.Python;
        } else {
            throw new UnsupportedOperationException("Unknown interpreter for script " + fileName);
        }
        sparkScript.setInterpreter(interpreter);
        return sparkScript;
    }

    protected Table toTable(String tableName, HdfsDataUnit jobTarget) {
        return SparkUtils.hdfsUnitToTable(tableName, jobTarget, yarnConfiguration, podId, customerSpace);
    }

    private ScriptJobConfig getScriptConfig() {
        ScriptJobConfig jobConfig = new ScriptJobConfig();
        jobConfig.setNumTargets(getNumTargets());
        jobConfig.setParams(getParams());
        return jobConfig;
    }

    /**
     * JSON schema determined by the spark script
     */
    protected JsonNode getParams() {
        return JsonUtils.deserialize("{}", JsonNode.class);
    }

    private int getNumTargets() {
        return 1;
    }

}
