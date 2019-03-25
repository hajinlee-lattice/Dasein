package com.latticeengines.query.service.impl;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.spark.InputStreamSparkScript;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.query.exposed.service.SparkSQLService;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;

@Service("sparkSQLService")
public class SparkSQLServiceImpl implements SparkSQLService {

    private static final Logger log = LoggerFactory.getLogger(SparkSQLServiceImpl.class);

    @Inject
    private LivySessionService livySessionService;

    @Inject
    private SparkJobService sparkJobService;

    @Inject
    private EMRCacheService emrCacheService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${dataflowapi.spark.driver.cores}")
    private int driverCores;

    @Value("${dataflowapi.spark.driver.mem}")
    private String driverMem;

    @Value("${dataflowapi.spark.executor.cores}")
    private int executorCores;

    @Value("${dataflowapi.spark.executor.mem}")
    private String executorMem;

    @Value("${dataflowapi.spark.max.executors}")
    private String maxExecutors;

    @Value("${dataflowapi.spark.min.executors}")
    private String minExecutors;

    @Override
    public LivySession initializeLivySession(AttributeRepository attrRepo, Map<String, String> hdfsPathMap) {
        String tenantId = attrRepo.getCustomerSpace().getTenantId();
        String jobName = tenantId + "~SparkSQL";
        String livyHost;
        if (Boolean.TRUE.equals(useEmr)) {
            livyHost = emrCacheService.getLivyUrl();
        } else {
            livyHost = "http://localhost:8998";
        }
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(context -> {
            LivySession session = livySessionService.startSession(livyHost, jobName, //
                    getLivyConf(), getSparkConf());
            bootstrapAttrRepo(session, hdfsPathMap);
            return session;
        });
    }

    @Override
    public long getCount(CustomerSpace customerSpace, LivySession livySession, String sql) {
        InputStreamSparkScript sparkScript = getQueryScript();
        ScriptJobConfig jobConfig = new ScriptJobConfig();
        jobConfig.setNumTargets(0);
        Map<String, Object> params = new HashMap<>();
        params.put("SQL", sql);
        params.put("SAVE", false);
        jobConfig.setParams(JsonUtils.convertValue(params, JsonNode.class));
        SparkJobResult result = sparkJobService.runScript(livySession, sparkScript, jobConfig);
        return Long.valueOf(result.getOutput());
    }

    @Override
    public HdfsDataUnit getData(CustomerSpace customerSpace, LivySession livySession, String sql) {
        InputStreamSparkScript sparkScript = getQueryScript();
        ScriptJobConfig jobConfig = new ScriptJobConfig();
        jobConfig.setNumTargets(1);
        Map<String, Object> params = new HashMap<>();
        params.put("SQL", sql);
        params.put("SAVE", true);
        jobConfig.setParams(JsonUtils.convertValue(params, JsonNode.class));
        String workspace = PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString();
        jobConfig.setWorkspace(workspace);
        SparkJobResult result = sparkJobService.runScript(livySession, sparkScript, jobConfig);
        return result.getTargets().get(0);
    }

    private void bootstrapAttrRepo(LivySession livySession, Map<String, String> hdfsPathMap) {
        InputStreamSparkScript sparkScript = getAttrRepoScript();
        ScriptJobConfig jobConfig = new ScriptJobConfig();
        jobConfig.setNumTargets(0);
        Map<String, Object> params = new HashMap<>();
        params.put("TABLE_MAP", hdfsPathMap);
        jobConfig.setParams(JsonUtils.convertValue(params, JsonNode.class));
        SparkJobResult result = sparkJobService.runScript(livySession, sparkScript, jobConfig);
        log.info("Output: " + result.getOutput());
    }

    private InputStreamSparkScript getAttrRepoScript() {
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/attrrepo.scala");
        InputStreamSparkScript sparkScript = new InputStreamSparkScript();
        sparkScript.setStream(is);
        sparkScript.setInterpreter(SparkInterpreter.Scala);
        return sparkScript;
    }

    private InputStreamSparkScript getQueryScript() {
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/query.scala");
        InputStreamSparkScript sparkScript = new InputStreamSparkScript();
        sparkScript.setStream(is);
        sparkScript.setInterpreter(SparkInterpreter.Scala);
        return sparkScript;
    }

    private Map<String, Object> getLivyConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("driverCores", driverCores);
        conf.put("driverMemory", driverMem);
        conf.put("executorCores", executorCores);
        conf.put("executorMemory", executorMem);
        return conf;
    }

    private Map<String, String> getSparkConf() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.executor.instances", String.valueOf(Math.max(Integer.valueOf(minExecutors), 1)));
        conf.put("spark.dynamicAllocation.minExecutors", minExecutors);
        conf.put("spark.dynamicAllocation.maxExecutors", maxExecutors);
        return conf;
    }

}
