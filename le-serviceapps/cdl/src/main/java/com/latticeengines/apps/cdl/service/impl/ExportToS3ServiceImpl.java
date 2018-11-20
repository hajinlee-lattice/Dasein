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

import com.latticeengines.apps.cdl.service.DataCollectionService;
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
    private DataCollectionService dataCollectionService;

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
        requests.forEach(r -> tenants.add(r.customerSpace.getTenantId()));

        log.info(String.format("Starting to export from hdfs to s3. tenantIds=%s, size=%s",
                StringUtils.join(tenants, ","), requests.size()));
        List<HdfsS3Exporter> exporters = new ArrayList<>();
        for (ExportRequest request : requests) {
            exporters.add(new HdfsS3Exporter(request));
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
                DataCollection dc = dataCollectionService.getDefaultCollection(customer);
                if (dc == null) {
                    log.info("There's no data collection for tenantId=" + tenantId);
                    continue;
                }
                DataCollection.Version activeVersion = dataCollectionService.getActiveVersion(customer);
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
        List<String> activeTableNames = dataCollectionService.getTableNames(customer, null, role, activeVersion);
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
        String tenantId = customerSpace.getTenantId();
        String hdfsMetadataDir = pathBuilder.getHdfsAtlasMetadataDir(podId, tenantId);
        String s3MetadataDir = pathBuilder.getS3AtlasMetadataDir(s3Bucket, tenantId);
        requests.add(new ExportRequest("metadata", hdfsMetadataDir, s3MetadataDir, customerSpace));

        String hdfsFilesDir = pathBuilder.getHdfsAtlasFilesDir(podId, tenantId);
        String s3FilesDir = pathBuilder.getS3AtlasFilesDir(s3Bucket, tenantId);
        requests.add(new ExportRequest("files", hdfsFilesDir, s3FilesDir, customerSpace));

        String hdfsTablesDir = pathBuilder.getHdfsAtlasTablesDir(podId, tenantId);
        String s3TablesDir = pathBuilder.getS3AtlasTablesDir(s3Bucket, tenantId);
        requests.add(new ExportRequest("tables", hdfsTablesDir, s3TablesDir, customerSpace));

        String hdfsTableSchemasDir = pathBuilder.getHdfsAtlasTableSchemasDir(podId, tenantId);
        String s3TableSchemasDir = pathBuilder.getS3AtlasTableSchemasDir(s3Bucket, tenantId);
        requests.add(new ExportRequest("table-schemas", hdfsTableSchemasDir, s3TableSchemasDir, customerSpace));
    }

    private void buildAnalyticsRequests(List<ExportRequest> requests, CustomerSpace customerSpace) {
        String hdfsDataDir = pathBuilder.getHdfsAnalyticsDataDir(customerSpace.toString());
        String s3DataDir = pathBuilder.getS3AnalyticsDataDir(s3Bucket, customerSpace.getTenantId());
        requests.add(new ExportRequest("analytics-data", hdfsDataDir, s3DataDir, customerSpace));

        String hdfsModelsDir = pathBuilder.getHdfsAnalyticsDataDir(customerSpace.toString());
        String s3ModelsDir = pathBuilder.getS3AnalyticsDataDir(s3Bucket, customerSpace.getTenantId());
        requests.add(new ExportRequest("analytics-models", hdfsModelsDir, s3ModelsDir, customerSpace));
    }

    private class HdfsS3Exporter implements Runnable {
        private String srcDir;
        private String tgtDir;
        private String customer;
        private String tenantId;
        private String name;

        HdfsS3Exporter(ExportRequest request) {
            this.srcDir = request.srcDir;
            this.tgtDir = request.tgtDir;
            this.customer = request.customerSpace.toString();
            this.tenantId = request.customerSpace.getTenantId();
            this.name = request.name;
        }

        @Override
        public void run() {
            try (PerformanceTimer timer = new PerformanceTimer( //
                    "Finished copying hdfs dir=" + srcDir + " to s3 dir=" + tgtDir)) {
                log.info("Start copying from hdfs dir=" + srcDir + " to s3 dir=" + tgtDir);
                try {
                    Configuration hadoopConfiguration = createConfiguration();
                    List<String> subFolders = new ArrayList<>();
                    if ("analytics-data".equals(name) || "analytics-models".equals(name)) {
                        HdfsUtils.getFilesForDir(hadoopConfiguration, srcDir).forEach(path -> {
                            String subFolder = path.substring(path.lastIndexOf("/"));
                            log.info("Found a " + name + " sub-folder for " + tenantId + " : " + subFolder);
                            subFolders.add(subFolder);
                        });
                        if (CollectionUtils.size(subFolders) < 200) {
                            subFolders.clear();
                        }
                    }
                    if ("tables".equals(name)) {
                        Set<String> tableNames = getTablesInCollection();
                        subFolders.addAll(tableNames);
                    }
                    if (CollectionUtils.isEmpty(subFolders)) {
                        subFolders.add("");
                    }
                    int count = 1;
                    List<String> failedFolders = new ArrayList<>();
                    for (String subFolder: subFolders) {
                        log.info(tenantId + ": start copying " + count + "/" + subFolders.size() //
                                + " sub-folder " + subFolder);
                        String srcPath = srcDir + subFolder;
                        String tgtPath = tgtDir + subFolder;
                        if ("tables".equals(name) && StringUtils.isNotBlank(subFolder)) {
                            Table table = metadataProxy.getTable(customer, subFolder);
                            if (table == null || CollectionUtils.isEmpty(table.getExtracts())) {
                                log.info(tenantId + ": finished copying " + (count++) + "/" + subFolders.size() //
                                        + " table " + subFolder + " as it is not part of the data collection.");
                                continue;
                            }
                            srcPath = table.getExtracts().get(0).getPath();
                            tgtPath = tgtDir + "/" + subFolder;
                        }
                        try {
                            if (HdfsUtils.fileExists(hadoopConfiguration, srcPath)) {
                                Configuration distcpConfiguration = createConfiguration(subFolder);
                                HdfsUtils.distcp(distcpConfiguration, srcPath, tgtPath, queueName);
                            } else {
                                log.info(srcPath + " does not exist, skip copying.");
                            }
                        } catch (Exception e) {
                            if (StringUtils.isNotBlank(name)) {
                                log.warn("Failed copy sub-folder " + subFolder + " in " + name);
                                failedFolders.add(subFolder);
                            }
                        }
                        log.info(tenantId + ": finished copying " + (count++) + "/" + subFolders.size() //
                                + " " + name + " sub-folder " + subFolder);
                    }
                    if (CollectionUtils.isNotEmpty(failedFolders)) {
                        throw new RuntimeException("Failed to copy sub-folders: " + StringUtils.join(failedFolders));
                    }
                } catch (Exception ex) {
                    String msg = String.format("Failed to copy hdfs dir=%s to s3 dir=%s for tenant=%s", srcDir, tgtDir,
                            customer);
                    throw new RuntimeException(msg, ex);
                }
            }
        }

        private Set<String> getTablesInCollection() {
            Set<String> tableNames = new HashSet<>();
            DataCollection.Version version = dataCollectionService.getActiveVersion(customer);
            for (TableRoleInCollection role: TableRoleInCollection.values()) {
                List<String> tblsForRole = dataCollectionService.getTableNames(customer, null, role, version);
                tableNames.addAll(tblsForRole);
            }
            return tableNames;
        }

        private Configuration createConfiguration() {
            Configuration hadoopConfiguration = new Configuration(distCpConfiguration);
            String jobName = tenantId + "~" + name;
            hadoopConfiguration.set(JobContext.JOB_NAME, jobName);
            return hadoopConfiguration;
        }

        private Configuration createConfiguration(String jobNameSuffix) {
            Configuration hadoopConfiguration = new Configuration(distCpConfiguration);
            String jobName = tenantId + "~" + name + "~" + jobNameSuffix;
            hadoopConfiguration.set(JobContext.JOB_NAME, jobName);
            return hadoopConfiguration;
        }

    }
}
