package com.latticeengines.query.service.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.spark.InputStreamSparkScript;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.query.exposed.service.SparkSQLService;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;

@Service("sparkSQLService")
public class SparkSQLServiceImpl implements SparkSQLService {

    private static final Logger log = LoggerFactory.getLogger(SparkSQLServiceImpl.class);

    private static final long GB = 1024 * 1024 * 1024;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private LivySessionService livySessionService;

    @Inject
    private SparkJobService sparkJobService;

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
    private int maxExecutors;

    @Value("${dataflowapi.spark.min.executors}")
    private int minExecutors;

    @Override
    public LivySession initializeLivySession(AttributeRepository attrRepo, Map<String, String> hdfsPathMap, //
                                             int scalingFactor, String storageLevel, String secondaryJobName) {
        String tenantId = attrRepo.getCustomerSpace().getTenantId();
        String jobName;
        if (StringUtils.isNotBlank(secondaryJobName)) {
            jobName = String.format("%s~SparkSQL~%s", tenantId, secondaryJobName);
        } else {
            jobName = String.format("%s~SparkSQL", tenantId);
        }
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(context -> {
            LivySession session = null;
            try {
                session = livySessionService.startSession(jobName, //
                        getLivyConf(scalingFactor), getSparkConf(scalingFactor));
                bootstrapAttrRepo(session, hdfsPathMap, storageLevel);
            } catch (Exception e) {
                log.warn("Failed to launch a new livy session.", e);
                if (session != null) {
                    livySessionService.stopSession(session);
                }
                throw e;
            }
            return session;
        });
    }

    @Override
    public void prepareForCrossSellQueries(LivySession livySession, //
                                           String periodName, String trxnTable, String storageLevel) {
        InputStreamSparkScript sparkScript = getTrxnScript();
        ScriptJobConfig jobConfig = new ScriptJobConfig();
        jobConfig.setNumTargets(0);
        Map<String, Object> params = new HashMap<>();
        params.put("TRXN_TABLE", trxnTable);
        params.put("PERIOD_NAME", periodName);
        if (StringUtils.isNotBlank(storageLevel)) {
            params.put("STORAGE_LEVEL", storageLevel);
        }
        jobConfig.setParams(JsonUtils.convertValue(params, JsonNode.class));
        sparkJobService.runScript(livySession, sparkScript, jobConfig);
    }

    @Override
    public long getCount(CustomerSpace customerSpace, LivySession livySession, String sql) {
        InputStreamSparkScript sparkScript = getQueryScript();
        ScriptJobConfig jobConfig = new ScriptJobConfig();
        jobConfig.setNumTargets(0);
        Map<String, Object> params = new HashMap<>();
        params.put("SQL", compressSql(sql));
        params.put("SAVE", false);
        jobConfig.setParams(JsonUtils.convertValue(params, JsonNode.class));
        SparkJobResult result = sparkJobService.runScript(livySession, sparkScript, jobConfig);
        return Long.valueOf(result.getOutput());
    }

    @Override
    public HdfsDataUnit getData(CustomerSpace customerSpace, LivySession livySession, String sql, //
                                Map<String, Map<Long, String>> decodeMapping) {
        InputStreamSparkScript sparkScript = getQueryScript();
        ScriptJobConfig jobConfig = new ScriptJobConfig();
        jobConfig.setNumTargets(1);
        Map<String, Object> params = new HashMap<>();
        params.put("SQL", compressSql(sql));
        params.put("DECODE_MAPPING", decodeMapping);
        params.put("SAVE", true);
        jobConfig.setParams(JsonUtils.convertValue(params, JsonNode.class));
        String workspace = PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString();
        jobConfig.setWorkspace(workspace);
        SparkJobResult result = sparkJobService.runScript(livySession, sparkScript, jobConfig);
        return result.getTargets().get(0);
    }

    private void bootstrapAttrRepo(LivySession livySession, Map<String, String> hdfsPathMap, String storageLevel) {
        InputStreamSparkScript sparkScript = getAttrRepoScript();
        ScriptJobConfig jobConfig = new ScriptJobConfig();
        jobConfig.setNumTargets(0);
        Map<String, Object> params = new HashMap<>();
        params.put("TABLE_MAP", hdfsPathMap);
        params.put("TABLE_FORMAT", getTableFormat(hdfsPathMap));
        if (StringUtils.isNotBlank(storageLevel)) {
            params.put("STORAGE_LEVEL", storageLevel);
        }
        jobConfig.setParams(JsonUtils.convertValue(params, JsonNode.class));
        SparkJobResult result = sparkJobService.runScript(livySession, sparkScript, jobConfig);
        log.info("Output: " + result.getOutput());
    }

    private Map<String, DataUnit.DataFormat> getTableFormat(Map<String, String> hdfsPathMap) {
        Map<String, DataUnit.DataFormat> fmtMap = new HashMap<>();
        hdfsPathMap.forEach((tbl, path) -> {
            if (isParquet(path)) {
                fmtMap.put(tbl, DataUnit.DataFormat.PARQUET);
            } else {
                fmtMap.put(tbl, DataUnit.DataFormat.AVRO);
            }
        });
        return fmtMap;
    }

    private boolean isParquet(String path) {
        if (path.endsWith(".parquet")) {
            return true;
        } else if (path.endsWith(".avro")) {
            return false;
        } else {
            String parquetGlob = PathUtils.toParquetGlob(path);
            try {
                return CollectionUtils.isNotEmpty(HdfsUtils.getFilesByGlob(yarnConfiguration, parquetGlob));
            } catch (IOException e) {
                throw new RuntimeException("Failed to expand glob " + parquetGlob);
            }
        }
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

    private InputStreamSparkScript getTrxnScript() {
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/trxn.scala");
        InputStreamSparkScript sparkScript = new InputStreamSparkScript();
        sparkScript.setStream(is);
        sparkScript.setInterpreter(SparkInterpreter.Scala);
        return sparkScript;
    }

    private Map<String, Object> getLivyConf(int scalingFactor) {
        Map<String, Object> conf = new HashMap<>();
        conf.put("driverCores", driverCores);
        conf.put("driverMemory", driverMem);
        conf.put("executorCores", executorCores);
        conf.put("executorMemory", executorMem);
        if (scalingFactor > 1) {
            // scale up first
            String unit = executorMem.substring(executorMem.length()-1);
            int val = Integer.parseInt(executorMem.replace(unit, ""));
            String newMem = String.format("%d%s", 2 * val, unit);
            log.info("Double executor memory to " + newMem + " based on scalingFactor=" + scalingFactor);
            conf.put("executorMemory", newMem);
        }

        return conf;
    }

    private Map<String, String> getSparkConf(int scalingFactor) {
        scalingFactor = Math.max(scalingFactor - 1, 1);
        Map<String, String> conf = new HashMap<>();

        // instances
        int minExe = minExecutors * scalingFactor;
        int maxExe = maxExecutors * scalingFactor;
        conf.put("spark.executor.instances", "1");
        conf.put("spark.dynamicAllocation.initialExecutors", String.valueOf(minExe));
        conf.put("spark.dynamicAllocation.minExecutors", String.valueOf(minExe));
        conf.put("spark.dynamicAllocation.maxExecutors", String.valueOf(maxExe));

        // partitions
        int partitions = Math.max(maxExe * executorCores * 2, 200);
        conf.put("spark.default.parallelism", String.valueOf(partitions));
        conf.put("spark.sql.shuffle.partitions", String.valueOf(partitions));

        // others
        conf.put("spark.driver.maxResultSize", "4g");
        conf.put("spark.jars.packages", "commons-io:commons-io:2.6");
        return conf;
    }

    private String compressSql(String sql) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (GzipCompressorOutputStream gzip = new GzipCompressorOutputStream(baos)) {
                gzip.write(sql.getBytes(Charset.forName("UTF-8")));
                gzip.close();
                return Base64.getEncoder().encodeToString(baos.toByteArray());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress sql: " + sql);
        }
    }

}
