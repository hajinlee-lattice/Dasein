package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.ExportToS3Service;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component("exportToS3Service")
public class ExportToS3ServiceImpl implements ExportToS3Service {

    private static final Logger log = LoggerFactory.getLogger(ExportToS3ServiceImpl.class);

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

    @Inject
    private EMREnvService emrEnvService;

    @Value("${camille.zk.pod.id:Default}")
    protected String podId;

    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private String queueName;

    private HdfsToS3PathBuilder pathBuilder;

    @PostConstruct
    public void init() {
        String queue = LedpQueueAssigner.getEaiQueueNameForSubmission();
        queueName = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
        pathBuilder = new HdfsToS3PathBuilder();

    }

    @Override
    public void buildRequests(CustomerSpace customerSpace, List<ExportRequest> requests) {
        buildAnalyticsRequests(requests, customerSpace);
        buildAtlasRequests(requests, customerSpace);
    }

    @Override
    public void executeRequests(List<ExportRequest> requests) {
        if (CollectionUtils.isEmpty(requests)) {
            log.info("There's no tenant dir found.");
            return;
        }
        List<String> tenants = new ArrayList<>();
        requests.forEach(r -> {
            tenants.add(r.customerSpace.getTenantId());
        });

        log.info(String.format("Starting to export from hdfs to s3. tenantIds=%s, size=%s",
                StringUtils.join(tenants, ","), requests.size()));
        List<HdfsS3Exporter> exporters = new ArrayList<>();
        for (int i = 0; i < requests.size(); i++) {
            exporters.add(new HdfsS3Exporter(requests.get(i)));
        }
        int threadPoolSize = Math.min(6, requests.size());
        ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("s3-export-history", threadPoolSize);
        ThreadPoolUtils.runRunnablesInParallel(executorService, exporters, (int) TimeUnit.DAYS.toMinutes(2), 10);
        executorService.shutdown();
        log.info(String.format("Finished to export from hdfs to s3. tenantIds=%s", StringUtils.join(tenants, ",")));
    }

    @Override
    public void buildDataUnits(List<ExportRequest> requests) {
        if (CollectionUtils.isEmpty(requests)) {
            log.info("There's no tenant dir found.");
            return;
        }
        Set<CustomerSpace> spaceSet = new HashSet<>();
        for (int i = 0; i < requests.size(); i++) {
            if (!spaceSet.add(requests.get(i).customerSpace)) {
                continue;
            }
            CustomerSpace customerSpace = requests.get(i).customerSpace;
            String customer = customerSpace.toString();
            String tenantId = customerSpace.getTenantId();
            try {
                DataCollection dc = dataCollectionProxy.getDefaultDataCollection(customer);
                if (dc == null) {
                    log.info("There's no data collection for tenantId=" + tenantId);
                    continue;
                }
                DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customer);
                for (TableRoleInCollection role : TableRoleInCollection.values()) {
                    addDataUnitsForRole(customer, tenantId, activeVersion, role);
                }
            } catch (Exception ex) {
                log.warn("Failed to get tables for tenantId=" + tenantId + " msg=" + ex.getMessage());
            }
        }
    }

    private void addDataUnitsForRole(String customer, String tenantId, DataCollection.Version activeVersion,
            TableRoleInCollection role) {
        List<String> activeTableNames = dataCollectionProxy.getTableNames(customer, role, activeVersion);
        if (CollectionUtils.isNotEmpty(activeTableNames)) {
            log.info("Start to add active tables for tenant=" + customer + " role=" + role.name());
            activeTableNames.forEach(t -> {
                String tgtDir = getTargetDir(customer, tenantId, t);
                if (StringUtils.isNotBlank(tgtDir)) {
                    registerDataUnit(customer, tenantId, t, tgtDir);
                }
            });
        }
    }

    private String getTargetDir(String customer, String tenantId, String tableName) {
        Table table = metadataProxy.getTable(customer, tableName);
        if (table == null) {
            log.warn("Can not find the table=" + tableName + " for tenant=" + customer);
            return null;
        }
        List<Extract> extracts = table.getExtracts();
        if (CollectionUtils.isEmpty(extracts) || StringUtils.isBlank(extracts.get(0).getPath())) {
            log.warn("Can not find extracts of the table=" + tableName + " for tenant=" + customer);
            return null;
        }
        String srcDir = pathBuilder.getFullPath(extracts.get(0).getPath());
        String tgtDir = pathBuilder.convertAtlasTableDir(srcDir, podId, tenantId, s3Bucket);
        return tgtDir;
    }

    private void registerDataUnit(String customer, String tenantId, String tableName, String tgtDir) {
        S3DataUnit unit = new S3DataUnit();
        unit.setTenant(tenantId);

        unit.setName(tableName);
        unit.setLinkedDir(tgtDir);
        DataUnit created = dataUnitProxy.create(customer, unit);
        log.info("Registered DataUnit: " + JsonUtils.pprint(created));
    }

    private void buildAtlasRequests(List<ExportRequest> requests, CustomerSpace customerSpace) {
        String hdfsAtlasDir = pathBuilder.getHdfsAtlasDir(podId, customerSpace.getTenantId());
        String s3AtlasDir = pathBuilder.getS3AtlasDir(s3Bucket, customerSpace.getTenantId());
        requests.add(new ExportRequest(hdfsAtlasDir, s3AtlasDir, customerSpace));
    }

    private void buildAnalyticsRequests(List<ExportRequest> requests, CustomerSpace customerSpace) {
        String hdfsAnalyticDir = pathBuilder.getHdfsAnalyticsDir(customerSpace.toString());
        String s3AnalyticDir = pathBuilder.getS3AnalyticsDir(s3Bucket, customerSpace.getTenantId());
        requests.add(new ExportRequest(hdfsAnalyticDir, s3AnalyticDir, customerSpace));
    }

    private class HdfsS3Exporter implements Runnable {
        private String srcDir;
        private String tgtDir;
        private String tableName;
        private String customer;
        private String tenantId;

        HdfsS3Exporter(ExportRequest request) {
            this.srcDir = request.srcDir;
            this.tgtDir = request.tgtDir;
            this.customer = request.customerSpace.toString();
            this.tenantId = request.customerSpace.getTenantId();
        }

        @Override
        public void run() {
            try (PerformanceTimer timer = new PerformanceTimer("Copying hdfs dir=" + srcDir + " to s3 dir=" + tgtDir)) {
                try {
                    Configuration hadoopConfiguration = createConfiguration();
                    if (HdfsUtils.fileExists(hadoopConfiguration, srcDir)) {
                        HdfsUtils.distcp(hadoopConfiguration, srcDir, tgtDir, queueName);
                    } else {
                        log.info(srcDir + " does not exist, skip copying.");
                    }
                } catch (Exception ex) {
                    String msg = String.format("Failed to copy hdfs dir=%s to s3 dir=%s for tenant=%s", srcDir, tgtDir,
                            customer);
                    throw new RuntimeException(msg, ex);
                }
            }
        }

        private Configuration createConfiguration() {
            Configuration hadoopConfiguration = new Configuration(distCpConfiguration);
            String jobName = StringUtils.isNotBlank(tableName) ? tenantId + "~" + tableName : tenantId;
            hadoopConfiguration.set(JobContext.JOB_NAME, jobName);
            return hadoopConfiguration;
        }

    }
}
