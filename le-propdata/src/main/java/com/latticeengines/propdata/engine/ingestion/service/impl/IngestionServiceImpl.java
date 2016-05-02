package com.latticeengines.propdata.engine.ingestion.service.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.propdata.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.propdata.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.propdata.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.core.IngestionNames;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.core.service.PropDataTenantService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.engine.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.propdata.engine.ingestion.service.IngestionNewProgressValidator;
import com.latticeengines.propdata.engine.ingestion.service.IngestionProgressService;
import com.latticeengines.propdata.engine.ingestion.service.IngestionService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("ingestionService")
public class IngestionServiceImpl implements IngestionService {
    private static Log log = LogFactory.getLog(IngestionServiceImpl.class);

    private static final String CSV_GZ = ".csv.gz";
    private static final String BOMBORA_FIREHOSE = "Bombora_Firehose_";

    @Autowired
    IngestionEntityMgr ingestionEntityMgr;

    @Autowired
    private IngestionProgressService ingestionProgressService;

    @Autowired
    private IngestionNewProgressValidator ingestionNewProgressValidator;

    @Autowired
    private PropDataTenantService propDataTenantService;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Override
    public List<IngestionProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        killFailedProgresses();
        ingestAll();
        return kickoffAll();
    }

    @Override
    public IngestionProgress ingestInternal(String ingestionName, IngestionRequest ingestionRequest,
            String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        Ingestion ingestion = getIngestionByName(ingestionName);
        IngestionProgress progress = ingestionProgressService.createPreprocessProgress(ingestion,
                ingestionRequest.getSubmitter(), ingestionRequest.getFileName());
        if (ingestionNewProgressValidator.isDuplicateProgress(progress)) {
            return ingestionProgressService.updateDuplicateProgress(progress);
        }
        return ingestionProgressService.saveProgress(progress);
    }

    @Override
    public Ingestion getIngestionByName(String ingestionName) {
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(ingestionName);
        if (ingestion == null) {
            throw new IllegalArgumentException(
                    "Ingestion " + ingestionName + " does not have configuration.");
        }
        return ingestion;
    }

    @Override
    public void killFailedProgresses() {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("Status", ProgressStatus.PROCESSING);
        List<IngestionProgress> progresses = ingestionProgressService.getProgressesByField(fields);
        for (IngestionProgress progress : progresses) {
            ApplicationId appId = ConverterUtils.toApplicationId(progress.getApplicationId());
            try {
                ApplicationReport report = YarnUtils.getApplicationReport(yarnConfiguration, appId);
                if (report == null || report.getYarnApplicationState() == null
                        || report.getYarnApplicationState().equals(YarnApplicationState.FAILED)
                        || report.getYarnApplicationState().equals(YarnApplicationState.KILLED)) {
                    progress = ingestionProgressService.updateProgress(progress)
                            .status(ProgressStatus.FAILED)
                            .errorMessage(
                                    "Found application status to be FAILED or KILLED in the scan")
                            .commit(true);
                    log.info("Kill progress: " + progress.toString());
                }
            } catch (YarnException | IOException e) {
                log.error("Failed to track application status for " + progress.getApplicationId()
                        + ". Error: " + e.toString());
                if (e.getMessage().contains("doesn't exist in the timeline store")) {
                    progress = ingestionProgressService.updateProgress(progress)
                            .status(ProgressStatus.FAILED)
                            .errorMessage("Failed to track application status in the scan")
                            .commit(true);
                    log.info("Kill progress: " + progress.toString());
                }
            }
        }
    }

    public void ingestAll() {
        List<Ingestion> ingestions = ingestionEntityMgr.findAll();
        List<IngestionProgress> progresses = new ArrayList<IngestionProgress>();
        Iterator<Ingestion> iter = ingestions.iterator();
        while (iter.hasNext()) {
            Ingestion ingestion = iter.next();
            if (!ingestionNewProgressValidator.isIngestionTriggered(ingestion)) {
                iter.remove();
            } else {
                log.info("Triggered Ingestion: " + ingestion.toString());
            }
            progresses.addAll(getPreprocessProgresses(ingestion));
        }
        progresses = ingestionNewProgressValidator.checkDuplicateProgresses(progresses);
        ingestionProgressService.saveProgresses(progresses);
    }

    public List<IngestionProgress> kickoffAll() {
        List<IngestionProgress> progresses = new ArrayList<IngestionProgress>();
        progresses.addAll(ingestionProgressService.getNewIngestionProgresses());
        progresses.addAll(ingestionProgressService.getRetryFailedProgresses());
        return submitAll(progresses);
    }

    public List<IngestionProgress> getPreprocessProgresses(Ingestion ingestion) {
        List<IngestionProgress> progresses = new ArrayList<IngestionProgress>();
        switch (ingestion.getIngestionCriteria()) {
        case ANY_MISSING_FILE:
            List<String> missingFiles = getMissingFiles(ingestion);
            for (String file : missingFiles) {
                IngestionProgress progress = ingestionProgressService.createPreprocessProgress(
                        ingestion, PropDataConstants.SCAN_SUBMITTER, file);
                progresses.add(progress);
            }
            break;
        default:
            throw new UnsupportedOperationException("Ingestion criteria "
                    + ingestion.getIngestionCriteria() + " is not supported.");
        }
        return progresses;
    }

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        List<String> targetFiles = getTargetFiles(ingestion);
        if (ingestion.getIngestionName().equals(IngestionNames.BOMBORA_FIREHOSE)) {
            targetFiles = filterBomboraFiles(targetFiles);
        }
        com.latticeengines.domain.exposed.camille.Path hdfsDest = hdfsPathBuilder
                .constructIngestionDir(ingestion.getIngestionName());
        Set<String> existingFiles = new HashSet<String>();
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, hdfsDest.toString())) {
                List<String> hdfsFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration,
                        hdfsDest.toString(), null);
                for (String file : hdfsFiles) {
                    if (!HdfsUtils.isDirectory(yarnConfiguration, file) && file.endsWith(CSV_GZ)) {
                        Path path = new Path(file);
                        existingFiles.add(path.getName());
                    }
                }

            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan hdfs directory " + hdfsDest.toString());
        }
        Iterator<String> iter = targetFiles.iterator();
        while (iter.hasNext()) {
            String file = iter.next();
            if (existingFiles.contains(file)) {
                iter.remove();
            } else {
                log.info("Found missing file to download: " + file);
            }
        }

        return targetFiles;
    }

    public List<IngestionProgress> submitAll(List<IngestionProgress> progresses) {
        List<IngestionProgress> submittedProgresses = new ArrayList<IngestionProgress>();
        Boolean serviceTenantBootstrapped = false;
        for (IngestionProgress progress : progresses) {
            try {
                if (!serviceTenantBootstrapped) {
                    propDataTenantService.bootstrapServiceTenant();
                    serviceTenantBootstrapped = true;
                }
                ApplicationId applicationId = submitWorkflow(progress);
                progress = ingestionProgressService.updateSubmittedProgress(progress,
                        applicationId.toString());
                submittedProgresses.add(progress);
                log.info("Submitted workflow for progress [" + progress + "]. ApplicationID = "
                        + applicationId.toString());
            } catch (Exception e) {
                log.error("Failed to submit workflow for progress " + progress, e);
            }
        }
        return submittedProgresses;
    }

    private ApplicationId submitWorkflow(IngestionProgress progress) {
        Ingestion ingestion = progress.getIngestion();
        ProviderConfiguration providerConfiguration = ingestion.getProviderConfiguration();
        return new IngestionWorkflowSubmitter() //
                .workflowProxy(workflowProxy) //
                .ingestionProgress(progress) //
                .ingestion(ingestion) //
                .providerConfiguration(providerConfiguration) //
                .submit();
    }

    @Override
    public List<String> getTargetFiles(Ingestion ingestion) {
        switch (ingestion.getIngestionType()) {
        case SFTP_TO_HDFS:
            return getSftpFiles((SftpConfiguration) ingestion.getProviderConfiguration());
        default:
            throw new RuntimeException(
                    "Ingestion type " + ingestion.getIngestionType() + " is not supported");
        }

    }

    private List<String> getSftpFiles(SftpConfiguration config) {
        try {
            JSch jsch = new JSch();
            Session session = jsch.getSession(config.getSftpUserName(), config.getSftpHost(),
                    config.getSftpPort());
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(CipherUtils.decrypt(config.getSftpPasswordEncrypted()));
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;
            sftpChannel.cd("." + config.getSftpDir());
            Vector files = sftpChannel.ls(".");
            List<String> fileSources = new ArrayList<String>();
            for (int i = 0; i < files.size(); i++) {
                ChannelSftp.LsEntry file = (ChannelSftp.LsEntry) files.get(i);
                if (!file.getAttrs().isDir()) {
                    fileSources.add(file.getFilename());
                }
            }
            sftpChannel.exit();
            session.disconnect();
            return fileSources;
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> filterBomboraFiles(List<String> files) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -1);
        String cutoffDate = format.format(cal.getTime());
        Iterator<String> iter = files.iterator();
        while (iter.hasNext()) {
            String file = iter.next();
            if (file.startsWith(BOMBORA_FIREHOSE) && file.endsWith(CSV_GZ)
                    && file.compareTo(BOMBORA_FIREHOSE + cutoffDate + CSV_GZ) >= 0) {
                continue;
            }
            iter.remove();
        }
        return files;
    }

}
