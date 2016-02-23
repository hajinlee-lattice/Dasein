package com.latticeengines.monitor.metric.service.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.MetricUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;
import com.latticeengines.monitor.exposed.metric.service.MetricService;

@Component("metricService")
public class MetricServiceInfluxDbImpl implements MetricService {

    private static final Log log = LogFactory.getLog(MetricServiceInfluxDbImpl.class);
    private static final String DB_CACHE_KEY = "InfluxDB";
    private LoadingCache<String, InfluxDB> dbConnectionCache;
    private ExecutorService executor = Executors.newCachedThreadPool();

    @Autowired
    private VersionManager versionManager;

    @Value("${monitor.influxdb.url:}")
    private String url;

    @Value("${monitor.influxdb.username:}")
    private String username;

    @Value("${monitor.influxdb.password:}")
    private String password;

    @Value("${monitor.influxdb.environment:Local}")
    private String environment;

    @Value("${monitor.influxdb.log.level:NONE}")
    private InfluxDB.LogLevel logLevel;

    private Boolean enabled = false;
    private Boolean forceDisabled = false;
    private static String hostname;

    @PostConstruct
    private void postConstruct() {
        if (StringUtils.isNotEmpty(url)) {
            try {
                buildDbConnectionCache();
                log.info("Enabled metric store at " + url);
                enabled = true;

                List<String> dbNames = listDatabases();
                for (MetricDB db : MetricDB.values()) {
                    if (!dbNames.contains(db.getDbName())) {
                        getInfluxDB().createDatabase(db.getDbName());
                        log.info("Creating MetricDB " + db.getDbName() + " because it is not in the influxDB.");
                    }

                    List<String> policies = listRetentionPolicies(db.getDbName());
                    for (RetentionPolicyImpl policy : RetentionPolicyImpl.values()) {
                        if (!policies.contains(policy.getName())) {
                            createRetentionPolicyIfNotExist(db, policy);
                            log.info("Creating Retention Policy " + policy.getName() + " in db " + db.getDbName());
                        }
                    }
                }

            } catch (Exception e) {
                log.warn("Had problem connecting fluxDb at " + url + ". Disable the metric service.", e);
                enabled = false;
            }

        } else {
            log.info("Disabled metric store because an empty url is provided.");
        }
    }

    @Override
    public <F extends Fact, D extends Dimension> void
    write(MetricDB db, Measurement<F, D> measurement) {
        write(db, Collections.singleton(measurement));
    }

    @Override
    public <F extends Fact, D extends Dimension> void write(MetricDB db,
            Collection<? extends Measurement<F, D>> measurements) {
        if (enabled) {
            log.info("Received " + measurements.size() + " points to write.");
            executor.submit(new MetricRunnable<>(db, measurements));
        } else if (!forceDisabled && StringUtils.isNotEmpty(url)) {
            postConstruct();
        }
    }

    @Override
    public void disable() {
        if (enabled) {
            enabled = false;
            forceDisabled = true;
            log.info("InfluxDB metric service is disabled.");
        }
    }

    private <F extends Fact, D extends Dimension> BatchPoints generateBatchPoints(MetricDB db,
            Collection<? extends Measurement<F, D>> measurements) {
        BatchPoints.Builder builder = BatchPoints.database(db.getDbName());
        builder.tag(MetricUtils.TAG_HOST, getHostName() == null ? MetricUtils.NULL : getHostName());
        builder.tag(MetricUtils.TAG_ENVIRONMENT, environment);
        builder.tag(MetricUtils.TAG_ARTIFACT_VERSION, StringUtils.isEmpty(versionManager.getCurrentVersion())
                ? MetricUtils.NULL : versionManager.getCurrentVersion());
        RetentionPolicy policy = RetentionPolicyImpl.DEFAULT;
        List<Point> points = new ArrayList<>();

        for (Measurement<F, D> measurement : measurements) {
            Fact fact = measurement.getFact();
            Dimension dimension = measurement.getDimension();
            policy = measurement.getRetentionPolicy();
            Point.Builder pointBuilder = Point.measurement(measurement.getClass().getSimpleName());
            pointBuilder = pointBuilder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            pointBuilder.tag(MetricUtils.parseTags(dimension));
            pointBuilder = pointBuilder.fields(MetricUtils.parseFields(fact));
            points.add(pointBuilder.build());
        }

        builder = builder.retentionPolicy(policy.getName());
        builder = builder.consistency(InfluxDB.ConsistencyLevel.ALL);
        builder.points(points.toArray(new Point[points.size()]));

        return builder.build();
    }

    @VisibleForTesting
    boolean isEnabled() {
        return enabled;
    }

    @VisibleForTesting
    InfluxDB getInfluxDB() {
        try {
            return dbConnectionCache.get(DB_CACHE_KEY);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to retrieve influxDB connection from cache.", e);
        }
    }

    private void buildDbConnectionCache() {
        dbConnectionCache = CacheBuilder.newBuilder().maximumSize(1).concurrencyLevel(1).weakKeys()
                .expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<String, InfluxDB>() {

                    @Override
                    public InfluxDB load(String key) {
                        if (DB_CACHE_KEY.equals(key)) {
                            String decryptedPassword = CipherUtils.decrypt(password);
                            InfluxDB influxDB = InfluxDBFactory.connect(url, username, decryptedPassword);
                            influxDB.setLogLevel(logLevel);
                            return influxDB;
                        } else {
                            return null;
                        }
                    }

                });
    }

    private class MetricRunnable<F extends Fact, D extends Dimension> implements Runnable {

        private MetricDB metricDb;
        private Collection<? extends Measurement<F, D>> measurements;

        MetricRunnable(MetricDB metricDb, Collection<? extends Measurement<F, D>> measurements) {
            this.metricDb = metricDb;
            this.measurements = measurements;
        }

        @Override
        public void run() {
            try {
                BatchPoints batchPoints = generateBatchPoints(metricDb, measurements);
                getInfluxDB().write(batchPoints);
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

    private void createRetentionPolicyIfNotExist(MetricDB db, RetentionPolicyImpl policy) {
        String queryString = String.format("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION %d",
                policy.getName(), db.getDbName(), policy.getDuration(), policy.getReplication());
        JsonNode jsonNode = queryInfluxDb(queryString, db.getDbName());
        if (jsonNode.get("results").get(0).has("error")) {
            throw new RuntimeException(
                    "Failed to create retention policy: " + jsonNode.get("results").get(0).get("error"));
        }
    }

    @VisibleForTesting
    List<String> listDatabases() {
        List<String> dbNames = new ArrayList<>();
        JsonNode jsonNode = queryInfluxDb("SHOW DATABASES", "_internal");
        JsonNode values = jsonNode.get("results").get(0).get("series").get(0).get("values");
        for (JsonNode node : values) {
            String dbName = node.get(0).asText();
            dbNames.add(dbName);
        }
        return dbNames;
    }

    @VisibleForTesting
    List<String> listRetentionPolicies(String dbName) {
        List<String> policies = new ArrayList<>();
        JsonNode jsonNode = queryInfluxDb("SHOW RETENTION POLICIES ON " + dbName, dbName);
        JsonNode values = jsonNode.get("results").get(0).get("series").get(0).get("values");
        for (JsonNode node : values) {
            String policy = node.get(0).asText();
            policies.add(policy);
        }
        return policies;
    }

    private JsonNode queryInfluxDb(String query, String dbName) {
        try {
            String response = HttpClientWithOptionalRetryUtils.sendGetRequest(url + "/query", false,
                    Collections.<BasicNameValuePair> emptyList(), new BasicNameValuePair("q", query),
                    new BasicNameValuePair("db", dbName));
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(response);
        } catch (IOException e) {
            throw new RuntimeException("Failed to query influxDb at " + url, e);
        }
    }

    private static String getHostName() {
        if (hostname == null || hostname.equals("unknown")) {
            try {
                InetAddress addr;
                addr = InetAddress.getLocalHost();
                hostname = addr.getHostName();
            } catch (UnknownHostException ex) {
                log.error("Hostname can not be resolved");
            }
        }
        return hostname;
    }

}
