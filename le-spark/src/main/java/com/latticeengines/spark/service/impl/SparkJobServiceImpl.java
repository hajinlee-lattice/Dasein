package com.latticeengines.spark.service.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.livy.scalaapi.LivyScalaClient;
import org.apache.livy.scalaapi.ScalaJobHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.InputStreamSparkScript;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.LocalFileSparkScript;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.SparkScript;
import com.latticeengines.domain.exposed.spark.SparkScriptOutput;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.spark.service.LivyClientService;

import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

@Service("sparkJobService")
public class SparkJobServiceImpl implements SparkJobService {

    private static final Logger log = LoggerFactory.getLogger(SparkJobServiceImpl.class);

    private static final String URI_SESSIONS = "/sessions";
    private static final String CELL_BREAKER_SCALA = "// -----CELL BREAKER----";
    private static final String CELL_BREAKER_PYTHON = "# -----CELL BREAKER----";

    @Inject
    private LivySessionService sessionService;

    @Inject
    private LivyClientService clientService;

    @Inject
    private Configuration yarnConfiguration;

    public <J extends AbstractSparkJob<C>, C extends SparkJobConfig> //
    SparkJobResult runJob(LivySession session, Class<J> jobClz, C config) {
        return runJob(session, jobClz, config, Collections.emptyList());
    }

    /**
     * No need to expose the ability of adding extra jars for now.
     */
    private <J extends AbstractSparkJob<C>, C extends SparkJobConfig> //
    SparkJobResult runJob(LivySession session, Class<J> jobClz, C config, Iterable<String> extraJars) {
        J job;
        try {
            job = jobClz.getConstructor().newInstance();
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Failed to instantiate a spark job of type " + jobClz);
        }
        cleanupTargetDirs(config.getTargets());
        job.configure(config);
        LivyScalaClient client = clientService.createClient(session.getSessionUrl(), extraJars);
        try (PerformanceTimer timer = new PerformanceTimer()) {
            log.info("Submitting spark job " + job.getClass().getSimpleName());
            String serialized = submitJobWithRetry(client, job);
            SparkJobResult result = JsonUtils.deserialize(serialized, SparkJobResult.class);
            String timerMsg = "Finished spark job " + job.getClass().getSimpleName();
            timer.setTimerMessage(timerMsg);
            return result;
        } finally {
            client.stop(false);
        }
    }

    private <J extends AbstractSparkJob<C>, C extends SparkJobConfig> //
    String submitJobWithRetry(LivyScalaClient client, J job) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(20);
        return retry.execute(context -> {
            if (context.getRetryCount() > 0) {
                log.info("Attempt=" + (context.getRetryCount() + 1) + ": retry submitting spark job " //
                        + job.getClass().getSimpleName() + " to livy session.");
            }
            try {
                ScalaJobHandle<String> handle = client.submit(job);
                return Await.result(handle, Duration.create(1, TimeUnit.DAYS));
            } catch (Exception e) {
                if (e.getMessage().contains("Unable to find class")) {
                    log.warn("Retry on error:\n" + e.getMessage());
                } else {
                    context.setExhaustedOnly();
                }
                throw new RuntimeException("Failed to execute spark job.", e);
            }
        });

    }

    @Override
    public SparkJobResult runScript(LivySession session, SparkScript script,
            ScriptJobConfig config) {
        LivySession retrieved = verifySession(session);
        cleanupTargetDirs(config.getTargets());
        SparkScriptClient client = getClient(retrieved, script);
        client.runPreScript(config);
        switch (script.getType()) {
            case InputStream:
                runInputStreamScript(client, (InputStreamSparkScript) script);
                break;
            case LocalFile:
                runLocalFileScript(client, (LocalFileSparkScript) script);
                break;
            default:
                throw new UnsupportedOperationException("Unknown script type " + script.getType());
        }
        SparkScriptOutput scriptOutput = client.runPostScript();
        SparkJobResult result = new SparkJobResult();
        result.setOutput(scriptOutput.getOutputStr());
        result.setTargets(scriptOutput.getOutput());
        return result;
    }

    private String runInputStreamScript(SparkScriptClient client, InputStreamSparkScript script) {
        InputStream stream = script.getStream();
        LineIterator lineIterator;
        try {
            lineIterator = IOUtils.lineIterator(stream, Charset.forName("UTF-8"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to iterate lines.", e);
        }
        return submitLines(client, lineIterator);
    }

    private String runLocalFileScript(SparkScriptClient client, LocalFileSparkScript script) {
        File file = script.getFile();
        LineIterator lineIterator;
        try {
            lineIterator = FileUtils.lineIterator(file);
        } catch (IOException e) {
            throw new RuntimeException("Failed to iterate lines.", e);
        }
        return submitLines(client, lineIterator);
    }

    private String submitLines(SparkScriptClient client, LineIterator lineIterator) {
        List<String> lines = new ArrayList<>();
        String output = null;
        for (String line : (Iterable<String>) () -> lineIterator) {
            lines.add(line);
            if (CELL_BREAKER_SCALA.equals(line) || CELL_BREAKER_PYTHON.equals(line)) {
                log.info("Find a cell breaker, going to submit " + CollectionUtils.size(lines) //
                        + " lines as one statement to spark.");
                output = submitLines(client, lines);
                lines.clear();
            }
        }
        if (CollectionUtils.isNotEmpty(lines)) {
            log.info("Submitting " + CollectionUtils.size(lines)
                    + " lines as one statement to spark.");
            output = submitLines(client, lines);
        }
        return output;
    }

    private String submitLines(SparkScriptClient client, List<String> lines) {
        String statement = StringUtils.join(lines, "\n");
        return client.runStatement(statement);
    }

    private SparkScriptClient getClient(LivySession session, SparkScript script) {
        String host = session.getHost();
        int id = session.getSessionId();
        String url = host + URI_SESSIONS + "/" + id;
        SparkInterpreter interpreter = script.getInterpreter();
        return new SparkScriptClient(interpreter, url);
    }

    private LivySession verifySession(LivySession session) {
        LivySession retrieved = sessionService.getSession(session);
        if (!LivySession.STATE_IDLE.equalsIgnoreCase(retrieved.getState())) {
            throw new IllegalStateException(
                    "Livy session is not ready: " + JsonUtils.serialize(retrieved));
        }
        return retrieved;
    }

    private void cleanupTargetDirs(List<HdfsDataUnit> targets) {
        if (targets != null) {
            targets.forEach(tgt -> {
                String path = tgt.getPath();
                if (path.length() < 10) {
                    throw new IllegalArgumentException("Suspicious target path [too short]: " + path);
                }
                try {
                    if (HdfsUtils.fileExists(yarnConfiguration, path)) {
                        HdfsUtils.rmdir(yarnConfiguration, path);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to clean up target path: " + path, e);
                }
            });
        }
    }

}
