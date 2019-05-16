package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.HdfsS3ImporterExporter;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.yarn.exposed.service.EMREnvService;

public abstract class BaseImportExportS3<T extends ImportExportS3StepConfiguration> extends BaseWorkflowStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseImportExportS3.class);
    private static final String SUCCESS_FILE = "_SUCCESS";

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    protected SourceFileProxy sourceFileProxy;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Resource(name = "distCpConfiguration")
    protected Configuration distCpConfiguration;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

    @Value("${aws.customer.export.s3.bucket}")
    protected String exportS3Bucket;

    @Inject
    private EMREnvService emrEnvService;

    @Value("${camille.zk.pod.id:Default}")
    protected String podId;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    protected ModelSummaryProxy modelSummaryProxy;

    private String queueName;
    protected String customer;
    protected String tenantId;
    protected DropBoxSummary dropBoxSummary;
    protected HdfsToS3PathBuilder pathBuilder;

    @Override
    public void onConfigurationInitialized() {
        String queue = LedpQueueAssigner.getEaiQueueNameForSubmission();
        queueName = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());

        customer = configuration.getCustomerSpace().toString();
        tenantId = configuration.getCustomerSpace().getTenantId();
        pathBuilder = new HdfsToS3PathBuilder(useEmr);
        dropBoxSummary = dropBoxProxy.getDropBox(configuration.getCustomerSpace().toString());
    }

    @Override
    public void execute() {
        List<ImportExportRequest> requests = new ArrayList<>();
        buildRequests(requests);
        if (CollectionUtils.isEmpty(requests)) {
            log.info("There's no source dir found.");
            return;
        }
        log.info("Starting to export from hdfs to s3 or vice versa. size=" + requests.size());
        List<HdfsS3ImporterExporter> exporters = new ArrayList<>();
        for (ImportExportRequest request : requests) {
            exporters.add(buildImporterExporter(request));
        }
        int threadPoolSize = Math.min(5, requests.size());
        ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("s3-import-export", threadPoolSize);
        ThreadPoolUtils.runRunnablesInParallel(executorService, exporters, (int) TimeUnit.DAYS.toMinutes(2), 10);
        executorService.shutdown();
        log.info("Finished to export from hdfs to s3 or vice versa.");
    }

    private HdfsS3ImporterExporter buildImporterExporter(ImportExportRequest request) {
        return new HdfsS3ImporterExporter(customer, distCpConfiguration, queueName, dataUnitProxy, request);
    }

    protected abstract void buildRequests(List<ImportExportRequest> requests);

    protected void addTableToRequestForImport(Table table, List<ImportExportRequest> requests) {
        List<Extract> extracts = table.getExtracts();
        if (CollectionUtils.isNotEmpty(extracts)) {
            extracts.forEach(extract -> {
                if (StringUtils.isNotBlank(extract.getPath())) {
                    String hdfsPath = pathBuilder.getFullPath(extract.getPath());
                    try {
                        if (!HdfsUtils.fileExists(distCpConfiguration, hdfsPath)) {
                            String s3Path = pathBuilder.convertAtlasTableDir(hdfsPath, podId, tenantId, s3Bucket);
                            if (isDoImport(s3Path, hdfsPath)) {
                                requests.add(
                                        new ImportExportRequest(s3Path, hdfsPath, table.getName(), false, false, true));
                            }
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to check Hdfs file=" + hdfsPath, ex);
                    }
                }
            });
        }
    }

    private boolean isDoImport(String s3Path, String hdfsPath) {
        try {
            boolean hasHdfsSuccess = HdfsUtils.fileExists(distCpConfiguration, getSuccessFile(hdfsPath));
            if (hasHdfsSuccess) {
                return false;
            }
            boolean hasS3Path = HdfsUtils.fileExists(distCpConfiguration, s3Path);
            if (!hasS3Path) {
                log.warn(String.format("There's No hdfs success path=%s, and there's No S3 path=%s", hdfsPath, s3Path));
                return false;
            }
            return true;
        } catch (Exception ex) {
            log.warn("Failed to check file=" + hdfsPath + " error=" + ex.getMessage());
            return false;
        }
    }

    private String getSuccessFile(String hdfsPath) {
        return hdfsPath + "/" + SUCCESS_FILE;
    }

}
