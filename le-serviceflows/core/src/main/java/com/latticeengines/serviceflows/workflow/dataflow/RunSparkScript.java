package com.latticeengines.serviceflows.workflow.dataflow;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkScriptStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.LocalFileSparkScript;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.SparkScript;
import com.latticeengines.spark.exposed.service.SparkJobService;

public abstract class RunSparkScript <S extends SparkScriptStepConfiguration> extends BaseSparkStep<S> {

    private static final Logger log = LoggerFactory.getLogger(RunSparkScript.class);

    @Inject
    private SparkJobService sparkJobService;

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

            computeScalingMultiplier(scriptConfig.getInput());

            String tenantId = customerSpace.getTenantId();
            String workspace = PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString();
            scriptConfig.setWorkspace(workspace);

            log.info("Spark script configuration: " + JsonUtils.serialize(scriptConfig));
            try {
                RetryTemplate retry = RetryUtils.getRetryTemplate(3);
                SparkJobResult result = retry.execute(context -> {
                    if (context.getRetryCount() > 0) {
                        log.warn("Previous failure:", context.getLastThrowable());
                        log.info("Attempt=" + (context.getRetryCount() + 1) + ": retry running spark script " //
                                + scriptPath);
                    }
                    LivySession session = createLivySession(tenantId + "~" + getClass().getSimpleName() //
                            + "~" + fileName);
                    return sparkJobService.runScript(session, script, scriptConfig);
                });
                postScriptExecution(result);
                try {
                    HdfsUtils.rmdir(yarnConfiguration, workspace);
                } catch (Exception e) {
                    log.warn("Failed to clean up workspace", e);
                }
            } finally {
                killLivySession();
            }
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
