package com.latticeengines.eai.runtime.service;

import java.lang.reflect.ParameterizedType;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.eai.service.EaiImportJobDetailService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public abstract class EaiRuntimeService<T extends EaiJobConfiguration> {

    private static Map<Class<? extends EaiJobConfiguration>, EaiRuntimeService<? extends EaiJobConfiguration>> map = new HashMap<>();

    protected Function<Float, Void> progressReporter;

    @Autowired
    private EaiImportJobDetailService eaiImportJobDetailService;

    @Autowired
    private WorkflowProxy workflowProxy;

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

    public void initJobDetail(String jobIdentifier, SourceType sourceType) {
        EaiImportJobDetail jobDetail = eaiImportJobDetailService
                .getImportJobDetailByCollectionIdentifier(jobIdentifier);
        if (jobDetail == null) {
            jobDetail = new EaiImportJobDetail();
            jobDetail.setStatus(ImportStatus.RUNNING);
            jobDetail.setSourceType(sourceType);
            jobDetail.setCollectionIdentifier(jobIdentifier);
            jobDetail.setProcessedRecords(0);
            jobDetail.setCollectionTimestamp(new Date());
            eaiImportJobDetailService.createImportJobDetail(jobDetail);
        } else {
            jobDetail.setStatus(ImportStatus.RUNNING);
            eaiImportJobDetailService.updateImportJobDetail(jobDetail);
        }
    }

    public void updateJobDetailStatus(String jobIdentifier, ImportStatus status) {
        EaiImportJobDetail jobDetail = eaiImportJobDetailService
                .getImportJobDetailByCollectionIdentifier(jobIdentifier);
        if (jobDetail != null) {
            jobDetail.setStatus(status);
            eaiImportJobDetailService.updateImportJobDetail(jobDetail);
        }
    }

    public void updateJobDetailExtractInfo(String jobIdentifier, String templateName, List<String> pathList,
            List<String> processedRecords, Long totalRows, Long ignoredRows, Long dedupedRows) {
        EaiImportJobDetail jobDetail = eaiImportJobDetailService
                .getImportJobDetailByCollectionIdentifier(jobIdentifier);
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
}
