package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.AccountMaster;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.AccountMasterRebuildConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationDataFlowService;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component("accountMasterRebuildService")
public class AccountMasterRebuildService extends AbstractFixedIntervalTransformationService
        implements TransformationService {
    private static final String DATA_FLOW_BEAN_NAME = "accountMasterRebuildFlow";

    private static final Log log = LogFactory.getLog(AccountMasterRebuildService.class);

    private static final String ACCOUNT_MASTER_FLOW = CollectionDataFlowKeys.REBUILD_FLOW; 

    private static final String VERSION = "VERSION";

    private boolean isManual = true;

    @Autowired
    private TransformationProgressEntityMgr progressEntityMgr;

    @Autowired
    private AccountMaster accountMaster;

    @Autowired
    private AccountMasterRebuildDataFlowService accountMasterRebuildDataFlowService;

    @Override
    public Source getSource() {
        return accountMaster;
    }

    @Override
    TransformationProgressEntityMgr getProgressEntityMgr() {
        return progressEntityMgr;
    }

    @Override
    Log getLogger() {
        return log;
    }

    @Override
    public boolean isManualTriggerred() {
        return true;
    }

    @Override
    protected void executeDataFlow(TransformationProgress progress, String workflowDir,
            TransformationConfiguration transformationConfiguration) {
        accountMasterRebuildDataFlowService.executeDataProcessing(accountMaster, workflowDir, progress.getVersion(),
                progress.getRootOperationUID(), DATA_FLOW_BEAN_NAME, transformationConfiguration);
    }

    @Override
    protected TransformationDataFlowService getTransformationDataFlowService() {
        return accountMasterRebuildDataFlowService;
    }

    @Override
    Date checkTransformationConfigurationValidity(TransformationConfiguration transformationConfiguration) {
        AccountMasterRebuildConfiguration conf = (AccountMasterRebuildConfiguration) transformationConfiguration;
        conf.getSourceConfigurations().put(VERSION, conf.getVersion());

        try {
            return HdfsPathBuilder.dateFormat.parse(conf.getVersion());
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    @Override
    TransformationConfiguration createNewConfiguration(List<String> latestBaseVersion, String newLatestVersion,
            List<SourceColumn> sourceColumns) {

        AccountMasterRebuildConfiguration configuration = new AccountMasterRebuildConfiguration();

        String version = HdfsPathBuilder.dateFormat.format(new Date());
        setAdditionalDetails(version, sourceColumns, configuration);

        return configuration;
    }

    @Override
    TransformationConfiguration readTransformationConfigurationObject(String confStr) throws IOException {
        return JsonUtils.deserialize(confStr, AccountMasterRebuildConfiguration.class);
    }

    @Override
    public Class<? extends TransformationConfiguration> getConfigurationClass() {
        return AccountMasterRebuildConfiguration.class;
    }

    @Override
    String workflowAvroDir(TransformationProgress progress) {
        return dataFlowDirInHdfs(progress, ACCOUNT_MASTER_FLOW);
    }

    /*
     * GOAL: Ensure that over the period of time missing versions and also
     * handled.
     *
     * LOGIC: return the first element (in high-to-low order) from
     * latestBaseVersions for which there is no entry in latestVersions list
     */
    @Override
    protected List<String> compareVersionLists(Source source, List<String> latestBaseVersions,
            List<String> latestVersions, String baseDir) {
        List<String> unprocessedBaseVersion = new ArrayList<>();

        if (isManual) {
            unprocessedBaseVersion.add(latestBaseVersions.get(0));
            return unprocessedBaseVersion;
        }

        for (String baseVersion : latestBaseVersions) {
            String pathForSuccessFlagLookup = baseDir + HDFS_PATH_SEPARATOR + baseVersion;
            boolean shouldSkip = false;
            if (latestVersions.size() == 0) {
                // if there is no version in source then then pick most recent
                // unprocessed version from base source version list

                shouldSkip = shouldSkipVersion(source, baseVersion, pathForSuccessFlagLookup);

                if (shouldSkip) {
                    continue;
                }

                unprocessedBaseVersion.add(baseVersion);
                return unprocessedBaseVersion;
            }

            boolean foundProcessedEntry = false;
            // try to find matching version in source version list
            for (String latestVersion : latestVersions) {
                log.debug("Compare: " + baseVersion);
                if (baseVersion.equals(latestVersion)) {
                    // if found equal version then skip further checking for
                    // this version and break from this inner loop
                    foundProcessedEntry = true;
                    break;
                } else {
                    if (baseVersion.compareTo(latestVersion) > 0) {
                        // if here, that means no corresponding version (equal)
                        // is found in source version list till now and in
                        // sorted order as soon as we see smaller version from
                        // base version than just break loop if any smaller
                        // version is seen
                        break;
                    }
                    continue;
                }
            }

            if (!foundProcessedEntry) {
                // if no equal version found in source version list then we
                // should process this base version as long as it is safe to
                // process it. Otherwise loop over to next base version
                shouldSkip = shouldSkipVersion(source, baseVersion, pathForSuccessFlagLookup);
                if (shouldSkip) {
                    continue;
                }

                // if we found a version that is smaller than baseVersion
                // then we return base version as this is a missing version
                unprocessedBaseVersion.add(baseVersion);
                return unprocessedBaseVersion;
            }
        }
        return unprocessedBaseVersion;
    }
}
