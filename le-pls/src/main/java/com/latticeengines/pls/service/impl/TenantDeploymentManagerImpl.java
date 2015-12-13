package com.latticeengines.pls.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataloader.JobStatus;
import com.latticeengines.domain.exposed.dataloader.LaunchJobsResult;
import com.latticeengines.domain.exposed.dataloader.QueryDataResult;
import com.latticeengines.domain.exposed.dataloader.QueryDataResult.QueryResultColumn;
import com.latticeengines.domain.exposed.dataloader.QueryStatusResult;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TenantDeployment;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStatus;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStep;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.service.DlCallback;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.pls.service.TenantDeploymentManager;
import com.latticeengines.pls.service.TenantDeploymentService;
import com.latticeengines.pls.service.VdbMetadataService;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.EmailService;
import com.latticeengines.security.exposed.service.UserService;

@Component("tenantDeploymentManager")
public class TenantDeploymentManagerImpl implements TenantDeploymentManager {

    private static final String importSfdcDataGroup = "LoadCRMDataForModeling";
    private static final String enrichDataGroup = "ModelBuild_PropDataMatch";
    private static final String profileSummaryQuery = "Profile_SummaryCounts";
    private static final String enrichmentSummaryQuery = "Profile_SummaryCounts";

    private static final Log log = LogFactory.getLog(TenantDeploymentManagerImpl.class);
    private static Map<String, LaunchJobsResult> importSfdcDataJobs = new HashMap<String, LaunchJobsResult>();
    private static Map<String, LaunchJobsResult> enrichDataJobs = new HashMap<String, LaunchJobsResult>();

    @Autowired
    private DataLoaderService dataLoaderService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantConfigService tenantConfigService;

    @Autowired
    private VdbMetadataService vdbMetadataService;

    @Autowired
    private TenantDeploymentService tenantDeploymentService;

    @Autowired
    private UserService userService;

    @Autowired
    private EmailService emailService;

    @Value("${security.pls.app.hostport:http://localhost:8080}")
    private String hostPort;

    @Override
    public void importSfdcData(String tenantId, TenantDeployment deployment) {
        try {
            executeGroup(tenantId, TenantDeploymentStep.IMPORT_SFDC_DATA, deployment);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18054, e, new String[] { e.getMessage() });
        }
    }

    @Override
    public void enrichData(String tenantId, TenantDeployment deployment) {
        try {
            executeGroup(tenantId, TenantDeploymentStep.ENRICH_DATA, deployment);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18055, e, new String[] { e.getMessage() });
        }
    }

    private void executeGroup(String tenantId, TenantDeploymentStep step, TenantDeployment deployment) throws Exception {
        if (importSfdcDataJobs.containsKey(tenantId) || enrichDataJobs.containsKey(tenantId)) {
            throw new LedpException(LedpCode.LEDP_18057);
        }

        Map<String, LaunchJobsResult> jobsMap = getBufferedJobsMap(step);
        synchronized (jobsMap) {
            jobsMap.put(tenantId, new LaunchJobsResult());
        }
        try {
            CustomerSpace space = CustomerSpace.parse(tenantId);
            String group = getGroupName(step);
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
            long launchId = dataLoaderService.executeLoadGroup(space.getTenantId(), group, dlUrl);

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            DlGroupRunnable runnable = new DlGroupRunnable(dlUrl, launchId, dataLoaderService);
            ProgressCallback progressCallback = new ProgressCallback(tenantId, jobsMap);
            runnable.setProgressCallback(progressCallback);
            CompletedCallback completedCallback = new CompletedCallback(tenantId, jobsMap);
            runnable.setCompletedCallback(completedCallback);
            executorService.execute(runnable);
            executorService.shutdown();

            deployment.setCurrentLaunchId(launchId);
            deployment.setStep(step);
            deployment.setStatus(TenantDeploymentStatus.IN_PROGRESS);
            deployment.setMessage(null);
            tenantDeploymentService.updateTenantDeployment(deployment);
        } catch (Exception e) {
            jobsMap.remove(tenantId);
            throw e;
        }
    }

    @Override
    public void validateMetadata(String tenantId, TenantDeployment deployment) {
        try {
            boolean missingMetadata = false;
            Tenant tenant = new Tenant();
            tenant.setId(tenantId);
            List<VdbMetadataField> fields = vdbMetadataService.getFields(tenant);
            for (VdbMetadataField field : fields) {
                if (isNullOrEmpty(field.getColumnName()) || isNullOrEmpty(field.getDisplayName())
                        || isNullOrEmpty(field.getApprovedUsage()) || isNullOrEmpty(field.getFundamentalType())) {
                    missingMetadata = true;
                    break;
                }
                if (isNullOrEmpty(field.getCategory()) && "Internal".equals(field.getTags())
                        && !"None".equals(field.getApprovedUsage())
                        && !"Model".equals(field.getApprovedUsage())) {
                    missingMetadata = true;
                    break;
                }
            }

            deployment.setStep(TenantDeploymentStep.VALIDATE_METADATA);
            deployment.setMessage(null);
            if (missingMetadata) {
                deployment.setStatus(TenantDeploymentStatus.WARNING);
            } else {
                deployment.setStatus(TenantDeploymentStatus.SUCCESS);
            }
            tenantDeploymentService.updateTenantDeployment(deployment);

            sendEmail(deployment);
        } catch (Exception e) {
            deployment.setStep(TenantDeploymentStep.VALIDATE_METADATA);
            deployment.setStatus(TenantDeploymentStatus.FAIL);
            deployment.setMessage(e.getMessage());
            tenantDeploymentService.updateTenantDeployment(deployment);

            sendEmail(deployment);

            throw new LedpException(LedpCode.LEDP_18056, e, new String[] { e.getMessage() });
        }
    }

    private boolean isNullOrEmpty(String value) {
        return value == null || value.length() == 0;
    }

    @Override
    public void cancelLaunch(String tenantId, long launchId) {
        String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
        dataLoaderService.cancelLaunch(launchId, dlUrl);
    }

    @Override
    public LaunchJobsResult getRunningJobs(String tenantId, TenantDeploymentStep step) {
        try
        {
            LaunchJobsResult jobs = null;
            Map<String, LaunchJobsResult> jobsMap = getBufferedJobsMap(step);
            synchronized (jobsMap) {
                jobs = jobsMap.get(tenantId);
            }

            if (jobs == null) {
                TenantDeployment deployment = tenantDeploymentService.getTenantDeployment(tenantId);
                if (deployment != null && deployment.getStep() == step) {
                    if (deployment.getStatus() == TenantDeploymentStatus.NEW ||
                            deployment.getStatus() == TenantDeploymentStatus.IN_PROGRESS) {
                        deployment.setStatus(TenantDeploymentStatus.FAIL);
                        deployment.setMessage("Buffered jobs were not found in memory when status is IN_PROGRESS.");
                        tenantDeploymentService.updateTenantDeployment(deployment);
                    }
                }

                jobs = new LaunchJobsResult();
                jobs.setLaunchStatus(JobStatus.SUCCESS);
            }
            return jobs;
        } catch(Exception e) {
            throw new LedpException(LedpCode.LEDP_18058, e, new String[] { step.toString(), e.getMessage() });
        }
    }

    @Override
    public String getStepSuccessTime(String tenantId, TenantDeploymentStep step) {
        try
        {
            CustomerSpace space = CustomerSpace.parse(tenantId);
            String group = getGroupName(step);
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
            return dataLoaderService.getLoadGroupLastSuccessTime(space.getTenantId(), group, dlUrl);
        } catch(Exception e) {
            throw new LedpException(LedpCode.LEDP_18059, e, new String[] { step.toString(), e.getMessage() });
        }
    }

    @Override
    public LaunchJobsResult getCompleteJobs(String tenantId, TenantDeploymentStep step) {
        try
        {
            CustomerSpace space = CustomerSpace.parse(tenantId);
            String group = getGroupName(step);
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
            long launchId = dataLoaderService.getLastFailedLaunchId(space.getTenantId(), group, dlUrl);
            return dataLoaderService.getLaunchJobs(launchId, dlUrl, false);
        } catch(Exception e) {
            throw new LedpException(LedpCode.LEDP_18060, e, new String[] { step.toString(), e.getMessage() });
        }
    }

    @Override
    public String runQuery(String tenantId, TenantDeploymentStep step) {
        try
        {
            String query;
            if (step == TenantDeploymentStep.IMPORT_SFDC_DATA) {
                query = profileSummaryQuery;
            } else if (step == TenantDeploymentStep.ENRICH_DATA) {
                query = enrichmentSummaryQuery;
            } else {
                throw new Exception(String.format("Deployment step %s was not supported.", step));
            }

            CustomerSpace space = CustomerSpace.parse(tenantId);
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
            return dataLoaderService.runQuery(space.getTenantId(), query, dlUrl);
        } catch(Exception e) {
            throw new LedpException(LedpCode.LEDP_18061, e, new String[] { e.getMessage() });
        }
    }

    @Override
    public QueryStatusResult getQueryStatus(String tenantId, String queryHandle) {
        try
        {
            CustomerSpace space = CustomerSpace.parse(tenantId);
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
            QueryStatusResult result = dataLoaderService.getQueryStatus(space.getTenantId(), queryHandle, dlUrl);
            return result;
        } catch(Exception e) {
            throw new LedpException(LedpCode.LEDP_18061, e, new String[] { e.getMessage() });
        }
    }

    @Override
    public void downloadQueryDataFile(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String tenantId, String queryHandle, String fileName) {
        try
        {
            CustomerSpace space = CustomerSpace.parse(tenantId);
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);

            int startRow = 0;
            QueryDataResult result;
            StringBuffer stringBuilder = new StringBuffer();
            do {
                result = dataLoaderService.getQueryData(space.getTenantId(), queryHandle, startRow, 2000, dlUrl);
                int rows = appendCsvData(stringBuilder, result, startRow == 0);
                if (rows == 0) {
                    break;
                }
                startRow += rows;
            } while (result.getRemainingRows() > 0);

            DlFileHttpDownloader downloader = new DlFileHttpDownloader(mimeType, fileName, stringBuilder.toString());
            downloader.downloadFile(request, response);
        } catch(Exception e) {
            throw new LedpException(LedpCode.LEDP_18062, e, new String[] { e.getMessage() });
        }
    }

    private int appendCsvData(StringBuffer stringBuffer, QueryDataResult result, boolean appendHeader) {
        List<QueryResultColumn> columns = result.getColumns();
        int columnCount = columns.size();

        if (appendHeader) {
            for (int i = 0; i < columnCount; i++) {
                String colName = "\"" + columns.get(i).getColumnName().replaceAll("\"", "\"\"") + "\"";
                if (i + 1 < columnCount) {
                    stringBuffer.append(colName + ",");
                } else {
                    stringBuffer.append(colName);
                }
            }
            stringBuffer.append("\r\n");
        }

        int rowCount = 0;
        for (QueryResultColumn column : columns) {
            List<String> values = column.getValues();
            if (values != null && values.size() > rowCount) {
                rowCount = values.size();
            }
        }
        for (int r = 0; r < rowCount; r++) {
            for (int c = 0; c < columnCount; c++) {
                List<String> values = columns.get(c).getValues();
                String value = null;
                if (r < values.size()) {
                    value = values.get(r);
                    if (value != null) {
                        value = "\"" + value.replaceAll("\"", "\"\"") + "\"";
                    }
                }
                if (c + 1 < columnCount) {
                    stringBuffer.append(value + ",");
                } else {
                    stringBuffer.append(value);
                }
            }
            stringBuffer.append("\r\n");
        }

        return rowCount;
    }

    private String getGroupName(TenantDeploymentStep step) throws Exception {
        switch (step) {
        case IMPORT_SFDC_DATA:
            return importSfdcDataGroup;
        case ENRICH_DATA:
            return enrichDataGroup;
        default:
            throw new Exception(String.format("Deployment step %s was not supported.", step));
        }
    }

    private Map<String, LaunchJobsResult> getBufferedJobsMap(TenantDeploymentStep step) throws Exception {
        switch (step) {
        case IMPORT_SFDC_DATA:
            return importSfdcDataJobs;
        case ENRICH_DATA:
            return enrichDataJobs;
        default:
            throw new Exception(String.format("Deployment step %s was not supported.", step));
        }
    }

    private void sendEmail(TenantDeployment deployment) {
        try {
            final String email = deployment.getCreatedBy();
            final TenantDeploymentStep step = deployment.getStep();
            final TenantDeploymentStatus status = deployment.getStatus();
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        User user = userService.findByEmail(email);
                        if (step == TenantDeploymentStep.IMPORT_SFDC_DATA) {
                            if (status == TenantDeploymentStatus.SUCCESS) {
                                emailService.sendPlsImportDataSuccessEmail(user, hostPort);
                            } else if (status == TenantDeploymentStatus.FAIL) {
                                emailService.sendPlsImportDataErrorEmail(user, hostPort);
                            }
                        } else if (step == TenantDeploymentStep.ENRICH_DATA) {
                            if (status == TenantDeploymentStatus.SUCCESS) {
                                emailService.sendPlsEnrichDataSuccessEmail(user, hostPort);
                            } else if (status == TenantDeploymentStatus.FAIL) {
                                emailService.sendPlsEnrichDataErrorEmail(user, hostPort);
                            }
                        } else if (step == TenantDeploymentStep.VALIDATE_METADATA) {
                            if (status == TenantDeploymentStatus.SUCCESS) {
                                emailService.sendPlsValidateMetadataSuccessEmail(user, hostPort);
                            } else if (status == TenantDeploymentStatus.WARNING) {
                                emailService.sendPlsMetadataMissingEmail(user, hostPort);
                            } else if (status == TenantDeploymentStatus.FAIL) {
                                emailService.sendPlsValidateMetadataErrorEmail(user, hostPort);
                            }
                        }
                    } catch (Exception e) { }
                }
            });
            executorService.shutdown();
        } catch (Exception e) { }
    }

    private class ProgressCallback implements DlCallback {
        private String tenantId;
        private Map<String, LaunchJobsResult> jobsMap;

        public ProgressCallback(String tenantId, Map<String, LaunchJobsResult> jobsMap) {
            this.tenantId = tenantId;
            this.jobsMap = jobsMap;
        }

        @Override
        public void callback(Object result) {
            synchronized (jobsMap) {
                jobsMap.put(tenantId, (LaunchJobsResult)result);
            }
        }
    }

    private class CompletedCallback implements DlCallback {
        private String tenantId;
        private Map<String, LaunchJobsResult> jobsMap;

        public CompletedCallback(String tenantId, Map<String, LaunchJobsResult> jobsMap) {
            this.tenantId = tenantId;
            this.jobsMap = jobsMap;
        }

        @Override
        public void callback(Object result) {
            TenantDeployment deployment = null;
            try
            {
                deployment = tenantDeploymentService.getTenantDeployment(tenantId);
                if (result instanceof Exception) {
                    Exception ex = (Exception)result;
                    handleError(deployment, ex.getMessage());
                } else {
                    LaunchJobsResult jobsResult = (LaunchJobsResult)result;
                    if (jobsResult.getLaunchStatus() == JobStatus.SUCCESS) {
                        if (deployment.getStep() == TenantDeploymentStep.ENRICH_DATA) {
                            deployment.setStatus(TenantDeploymentStatus.SUCCESS);
                            sendEmail(deployment);

                            try {
                                validateMetadata(tenantId, deployment);
                            } catch (Exception e) { }
                        } else {
                            handleSuccess(deployment);
                        }
                    } else {
                        handleError(deployment, jobsResult.getLaunchMessage());
                    }
                }

                synchronized (jobsMap) {
                    jobsMap.remove(tenantId);
                }

                startNextStep(deployment);
            } catch (Exception ex) {
                try {
                    synchronized (jobsMap) {
                        jobsMap.remove(tenantId);
                    }

                    if (deployment != null) {
                        handleError(deployment, ex.getMessage());
                    }
                } catch (Exception e) { }

                log.warn(String.format("Executing completed callback encountered an exception in tenant deployment. Tenant id: %d.", tenantId), ex);
            }
        }

        private void startNextStep(TenantDeployment deployment) {
            if (deployment.getStatus() == TenantDeploymentStatus.SUCCESS) {
                if (deployment.getStep() == TenantDeploymentStep.IMPORT_SFDC_DATA) {
                    try {
                        enrichData(tenantId, deployment);
                    } catch (Exception ex) {
                        deployment.setStep(TenantDeploymentStep.ENRICH_DATA);
                        handleError(deployment, ex.getMessage());
                    }
                }
            }
        }

        private void handleSuccess(TenantDeployment deployment) {
            deployment.setStatus(TenantDeploymentStatus.SUCCESS);
            tenantDeploymentService.updateTenantDeployment(deployment);

            sendEmail(deployment);
        }

        private void handleError(TenantDeployment deployment, String errorMessage) {
            deployment.setStatus(TenantDeploymentStatus.FAIL);
            deployment.setMessage(errorMessage);
            tenantDeploymentService.updateTenantDeployment(deployment);

            sendEmail(deployment);
        }
    }

}
