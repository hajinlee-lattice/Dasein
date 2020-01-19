package com.latticeengines.eai.runtime.service;

import static com.latticeengines.eai.util.HdfsUriGenerator.EXTRACT_DATE_FORMAT;

import java.lang.reflect.ParameterizedType;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.eai.service.EaiImportJobDetailService;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public abstract class EaiRuntimeService<T extends EaiJobConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(EaiRuntimeService.class);

    private static Map<Class<? extends EaiJobConfiguration>, EaiRuntimeService<? extends EaiJobConfiguration>> map = new HashMap<>();

    protected Function<Float, Void> progressReporter;

    @Autowired
    protected EaiImportJobDetailService eaiImportJobDetailService;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    protected JobProxy jobProxy;

    @Autowired
    protected DataFeedProxy dataFeedProxy;

    @SuppressWarnings("unchecked")
    public EaiRuntimeService() {
        map.put((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], this);
    }

    public static EaiRuntimeService<? extends EaiJobConfiguration> getRunTimeService(
            Class<? extends EaiJobConfiguration> clz) {
        return map.get(clz);
    }

    public abstract void invoke(T config);

    public void setProgressReporter(Function<Float, Void> progressReporter) {
        this.progressReporter = progressReporter;
    }

    protected void setProgress(float progress) {
        progressReporter.apply(progress);
    }

    protected void initJobDetail(Long jobDetailId, String jobIdentifier, SourceType sourceType) {
        initJobDetail(jobDetailId, jobIdentifier, sourceType, "");
    }

    protected void initJobDetail(Long jobDetailId, String jobIdentifier, SourceType sourceType, String fileName) {
        EaiImportJobDetail jobDetail = eaiImportJobDetailService
                .getImportJobDetailById(jobDetailId);
        if (jobDetail == null) {
            jobDetail = new EaiImportJobDetail();
            jobDetail.setStatus(ImportStatus.RUNNING);
            jobDetail.setSourceType(sourceType);
            jobDetail.setCollectionIdentifier(jobIdentifier);
            jobDetail.setProcessedRecords(0);
            jobDetail.setCollectionTimestamp(new Date());
            if (StringUtils.isNotEmpty(fileName)) {
                jobDetail.setImportFileName(fileName);
            }
            eaiImportJobDetailService.createImportJobDetail(jobDetail);
        } else {
            jobDetail.setStatus(ImportStatus.RUNNING);
            if (StringUtils.isNotEmpty(fileName)) {
                jobDetail.setImportFileName(fileName);
            }
            eaiImportJobDetailService.updateImportJobDetail(jobDetail);
        }
    }

    public void updateJobDetailStatus(Long jobDetailId, ImportStatus status) {
        EaiImportJobDetail jobDetail = eaiImportJobDetailService
                .getImportJobDetailById(jobDetailId);
        if (jobDetail != null) {
            jobDetail.setStatus(status);
            eaiImportJobDetailService.updateImportJobDetail(jobDetail);
        }
    }

    public void updateJobDetailExtractInfo(Long jobDetailId, String templateName, List<String> pathList,
            List<String> processedRecords, Long totalRows, Long ignoredRows, Long dedupedRows) {
        EaiImportJobDetail jobDetail = eaiImportJobDetailService
                .getImportJobDetailById(jobDetailId);
        if (jobDetail != null) {
            int totalRecords = 0;
            for (String processedRecord : processedRecords) {
                totalRecords += Integer.parseInt(processedRecord);
            }
            jobDetail.setProcessedRecords(totalRecords);
            jobDetail.setTemplateName(templateName);
            jobDetail.setPathDetail(pathList);
            jobDetail.setPRDetail(processedRecords);
            jobDetail.setTotalRows(totalRows);
            jobDetail.setIgnoredRows(ignoredRows);
            jobDetail.setDedupedRows(dedupedRows);
            //when extract has processed records info means the import completed, waiting for register.
            jobDetail.setStatus(ImportStatus.WAITINGREGISTER);
            eaiImportJobDetailService.updateImportJobDetail(jobDetail);
        }
    }

    public String createTempTable(String customerSpace, Table template, List<String> pathList,
                                  List<String> processedRecords) {
        Table tempTable = TableUtils.clone(template, NamingUtils.uuid("TempTable"));
        for (int i = 0; i < pathList.size(); i++) {
            tempTable.addExtract(createExtract(pathList.get(i), Long.parseLong(processedRecords.get(i))));
        }
        metadataProxy.createTable(customerSpace, tempTable.getName(), tempTable);
        return tempTable.getName();
    }

    private Extract createExtract(String path, long processedRecords) {
        Extract e = new Extract();
        e.setName(StringUtils.substringAfterLast(path, "/"));
        e.setPath(PathUtils.stripoutProtocol(path));
        e.setProcessedRecords(processedRecords);
        String dateTime = StringUtils.substringBetween(path, "/Extracts/", "/");
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        try {
            e.setExtractionTimestamp(f.parse(dateTime).getTime());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return e;
    }

    public String getTaskIdFromJobId(Long jobDetailId) {
        EaiImportJobDetail jobDetail = eaiImportJobDetailService
                .getImportJobDetailById(jobDetailId);
        if (jobDetail != null) {
            return jobDetail.getCollectionIdentifier();
        } else {
            return null;
        }
    }

    protected String createTargetPath(CustomerSpace customerSpace, BusinessEntity entity, SourceType sourceType) {
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-%s/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                sourceType.getName(),
                entity.name(),
                new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date()));
        return targetPath;
    }

    public JobStatus waitForWorkflowStatus(String applicationId, boolean running) {

        int retryOnException = 4;
        Job job = null;

        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
            } catch (Exception e) {
                System.out.println(String.format("Workflow job exception: %s", e.getMessage()));

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                return job.getJobStatus();
            }

            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected void waitForAppId(String appId) {
        log.info(String.format("Waiting for appId: %s", appId));

        com.latticeengines.domain.exposed.dataplatform.JobStatus status;
        int maxTries = 17280; // Wait maximum 24 hours
        int i = 0;
        do {
            status = jobProxy.getJobStatus(appId);
            SleepUtils.sleep(5000L);
            i++;
            if (i == maxTries) {
                break;
            }
        } while (!YarnUtils.TERMINAL_STATUS.contains(status.getStatus()));

        if (status.getStatus() != FinalApplicationStatus.SUCCEEDED) {
            throw new LedpException(LedpCode.LEDP_28015, new String[] { appId, status.getStatus().toString() });
        }

    }
}
