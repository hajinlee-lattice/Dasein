package com.latticeengines.pls.service.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.ScoringJobService;
import com.latticeengines.pls.workflow.ImportAndRTSBulkScoreWorkflowSubmitter;
import com.latticeengines.pls.workflow.RTSBulkScoreWorkflowSubmitter;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("scoringJobService")
public class ScoringJobServiceImpl implements ScoringJobService {
    private static final Logger log = LoggerFactory.getLogger(ScoringJobServiceImpl.class);

    @Value("${pls.scoring.use.rtsapi}")
    private boolean useRtsApiDefaultValue;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private RTSBulkScoreWorkflowSubmitter rtsBulkScoreWorkflowSubmitter;

    @Inject
    private ImportAndRTSBulkScoreWorkflowSubmitter importAndRTSBulkScoreWorkflowSubmitter;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private ImportFromS3Service importFromS3Service;

    private HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();

    @Override
    public List<Job> getJobs(String modelId) {
        Tenant tenantWithPid = getTenant();
        log.debug("Finding jobs for " + tenantWithPid.toString() + " with pid " + tenantWithPid.getPid() + " and model "
                + modelId);
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid);
        List<Job> ret = new ArrayList<>();
        ModelSummary modelSummary = modelSummaryProxy.findByModelId(MultiTenantContext.getTenant().getId(),
                modelId, false, false, true);
        String jobModelName = modelSummary != null ? modelSummary.getDisplayName() : null;
        for (Job job : jobs) {
            if (job.getJobType() == null) {
                continue;
            }

            if (job.getJobType().equals("scoreWorkflow") || job.getJobType().equals("importMatchAndScoreWorkflow")
                    || job.getJobType().equals("rtsBulkScoreWorkflow")
                    || job.getJobType().equals("importAndRTSBulkScoreWorkflow")) {
                String jobModelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
                if (jobModelId != null && jobModelId.equals(modelId)) {
                    job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, jobModelName);
                    ret.add(job);
                }
            }
        }
        return ret;

    }

    @Override
    public InputStream getScoreResults(String workflowJobId) {
        return quoteProtectionValues(getResultFile(workflowJobId, WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH));
    }

    private InputStream  quoteProtectionValues(InputStream inputStream) {
        StringBuilder sb = new StringBuilder();
        try (InputStreamReader reader = new InputStreamReader(
                new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8,
                        ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE,
                        ByteOrderMark.UTF_32BE), StandardCharsets.UTF_8)) {
            try (CSVParser parser = new CSVParser(reader, LECSVFormat.format)) {
                try (CSVPrinter printer = new CSVPrinter(sb,
                        CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)
                                .withHeader(parser.getHeaderMap().keySet().toArray(new String[] {})))) {
                    for (CSVRecord record : parser) {
                        for (String val : record) {
                            printer.print(val != null ? val : "");
                        }
                        printer.println();
                    }
                }
            }
        }
        catch (IOException e) {
            log.error("Error reading the input stream.");
            e.printStackTrace();
            return inputStream;
        }

        return IOUtils.toInputStream(sb.toString(), Charset.defaultCharset());
    }

    @Override
    public InputStream getPivotScoringFile(String workflowJobId) {
        return getResultFile(workflowJobId, WorkflowContextConstants.Outputs.PIVOT_SCORE_EVENT_EXPORT_PATH);
    }

    private InputStream getResultFile(String workflowJobId, String resultFileType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Job job = workflowProxy.getWorkflowExecution(workflowJobId,
                customerSpace != null ? customerSpace.toString() : null);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_18104, new String[] { workflowJobId });
        }

        String path = job.getOutputs().get(resultFileType);

        if (path == null) {
            throw new LedpException(LedpCode.LEDP_18103, new String[] { workflowJobId });
        }

        try {
            String hdfsDir = StringUtils.substringBeforeLast(path, "/");
            String filePrefix = StringUtils.substringAfterLast(path, "/");
            InputStream s3Stream = getResultStreamFromS3(hdfsDir, filePrefix);
            if (s3Stream == null) {
                List<String> paths = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, filePrefix + ".*");
                if (CollectionUtils.isEmpty(paths)) {
                    throw new LedpException(LedpCode.LEDP_18103, new String[]{workflowJobId});
                }
                return HdfsUtils.getInputStream(yarnConfiguration, paths.get(0));
            } else {
                return s3Stream;
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18102, e, new String[] { workflowJobId });
        }
    }

    private InputStream getResultStreamFromS3(String hdfsDir, String filePrefix) {
        String s3Dir = pathBuilder.exploreS3FilePath(hdfsDir, "bucket");
        if (s3Dir.endsWith("/Exports")) {
            s3Dir = StringUtils.substringBeforeLast(s3Dir, "/");
        }
        HdfsUtils.HdfsFilenameFilter fileFilter = filePath -> {
            if (filePath == null) {
                return false;
            }
            String name = FilenameUtils.getName(filePath);
            return name.matches(filePrefix + ".*.csv");
        };
        List<String> matchedFiles = importFromS3Service.getFilesForDir(s3Dir, fileFilter);
        if (CollectionUtils.isNotEmpty(matchedFiles)) {
            String key = pathBuilder.stripProtocolAndBucket(matchedFiles.get(0));
            log.debug("Streaming out S3 object " + key);
            return importFromS3Service.getS3FileInputStream(key);
        } else {
            log.warn("Did not find file with prefix " + filePrefix + " in s3 folder " + s3Dir + " either.");
            return null;
        }
    }

    @Override
    public String getResultScoreFileName(String workflowJobId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Job job = workflowProxy.getWorkflowExecution(workflowJobId,
                customerSpace != null ? customerSpace.toString() : null);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_18104, new String[] { workflowJobId });
        }

        String path = job.getOutputs().get(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH);

        if (path == null) {
            throw new LedpException(LedpCode.LEDP_18103, new String[] { workflowJobId });
        }
        String fileName = StringUtils.substringAfterLast(path, "/");
        if (!fileName.contains("_scored")) {
            return "scored.csv";
        }
        return StringUtils.substringBeforeLast(fileName, "_scored") + "_scored.csv";
    }

    @Override
    public String getResultPivotScoreFileName(String workflowJobId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Job job = workflowProxy.getWorkflowExecution(workflowJobId,
                customerSpace != null ? customerSpace.toString() : null);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_18104, new String[] { workflowJobId });
        }

        String path = job.getOutputs().get(WorkflowContextConstants.Outputs.PIVOT_SCORE_EVENT_EXPORT_PATH);

        if (path == null) {
            throw new LedpException(LedpCode.LEDP_18103, new String[] { workflowJobId });
        }
        String fileName = StringUtils.substringAfterLast(path, "/");
        return StringUtils.substringBeforeLast(fileName, "_pivoted") + "_pivoted.csv";
    }

    @Override
    public String scoreTestingData(String modelId, String fileName, Boolean performEnrichment, Boolean debug) {
        ModelSummary modelSummary = modelSummaryProxy.getByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        boolean enableLeadEnrichment =  Boolean.TRUE.equals(performEnrichment);
        boolean enableDebug = Boolean.TRUE.equals(debug);
        return scoreTestingDataUsingRtsApi(modelSummary, fileName, enableLeadEnrichment, enableDebug);
    }

    @Override
    public String scoreTrainingData(String modelId, Boolean performEnrichment, Boolean debug) {
        ModelSummary modelSummary = modelSummaryProxy.getByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        boolean enableLeadEnrichment =  Boolean.TRUE.equals(performEnrichment);
        boolean enableDebug = Boolean.TRUE.equals(debug);
        return scoreTrainingDataUsingRtsApi(modelSummary, enableLeadEnrichment, enableDebug);
    }

    private Tenant getTenant() {
        Tenant tenant = MultiTenantContext.getTenant();
        return tenantEntityMgr.findByTenantId(tenant.getId());
    }

    private String scoreTrainingDataUsingRtsApi(ModelSummary modelSummary, boolean enableLeadEnrichment,
                                                boolean debug) {
        if (modelSummary.getTrainingTableName() == null) {
            throw new LedpException(LedpCode.LEDP_18100, new String[] { modelSummary.getId() });
        }

        return rtsBulkScoreWorkflowSubmitter.submit(modelSummary.getId(), modelSummary.getTrainingTableName(),
                enableLeadEnrichment, "Training Data", debug).toString();
    }

    private String scoreTestingDataUsingRtsApi(ModelSummary modelSummary, String fileName, boolean enableLeadEnrichment,
                                               boolean debug) {
        return importAndRTSBulkScoreWorkflowSubmitter
                .submit(modelSummary.getId(), fileName, enableLeadEnrichment, debug).toString();
    }

    @Override
    public InputStream getScoringErrorStream(String workflowJobId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Job job = workflowProxy.getWorkflowExecution(workflowJobId,
                customerSpace != null ? customerSpace.toString() : null);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_18104, new String[] { workflowJobId });
        }

        String path = job.getOutputs().get(WorkflowContextConstants.Outputs.ERROR_OUTPUT_PATH);

        try {
            if (path == null) {
                return new ByteArrayInputStream(new byte[0]);
            }
            return HdfsUtils.getInputStream(yarnConfiguration, path);

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18102, e, new String[] { workflowJobId });
        }
    }
}
