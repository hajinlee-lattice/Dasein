package com.latticeengines.propdata.engine.ingestion.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.propdata.ingestion.Protocol;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
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

    private static final String CSV_GZ = "csv.gz";

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
        IngestionProgress progress = ingestionProgressService.createStagingIngestionProgress(
                ingestion, ingestionRequest.getSubmitter(), ingestionRequest.getFileName());
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

    public void ingestAll() {
        List<Ingestion> ingestions = ingestionEntityMgr.findAll();
        List<IngestionProgress> progresses = new ArrayList<IngestionProgress>();
        Iterator<Ingestion> iter = ingestions.iterator();
        while (iter.hasNext()) {
            Ingestion ingestion = iter.next();
            if (!ingestionNewProgressValidator.isIngestionTriggered(ingestion)) {
                iter.remove();
            }
            log.info("Triggered Ingestion: " + ingestion.toString());
            progresses.addAll(getStagingIngestionProgresses(ingestion));
        }
        progresses = ingestionNewProgressValidator.checkDuplcateProgresses(progresses);
        ingestionProgressService.saveProgresses(progresses);
    }

    public List<IngestionProgress> kickoffAll() {
        List<IngestionProgress> progresses = new ArrayList<IngestionProgress>();
        progresses.addAll(ingestionProgressService.getNewIngestionProgresses());
        progresses.addAll(ingestionProgressService.getRetryFailedProgresses());
        return submitAll(progresses);
    }

    public List<IngestionProgress> getStagingIngestionProgresses(Ingestion ingestion) {
        List<IngestionProgress> progresses = new ArrayList<IngestionProgress>();
        switch (ingestion.getIngestionCriteria()) {
        case ANY_MISSING_FILE:
            List<String> missingFiles = getMissingFiles(ingestion);
            for (String file : missingFiles) {
                IngestionProgress progress = ingestionProgressService
                        .createStagingIngestionProgress(ingestion, PropDataConstants.SCAN_SUBMITTER,
                                file);
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
        List<String> targetFiles = ingestion.getProtocol().getAllFiles();
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
        Protocol protocol = ingestion.getProtocol();
        return new IngestionWorkflowSubmitter() //
                .workflowProxy(workflowProxy) //
                .ingestionProgress(progress) //
                .ingestion(ingestion) //
                .protocol(protocol) //
                .submit();
    }

}
