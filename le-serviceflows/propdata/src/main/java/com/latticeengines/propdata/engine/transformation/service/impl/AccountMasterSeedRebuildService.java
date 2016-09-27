package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.AccountMasterSeed;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.AccountMasterSeedConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component("accountMasterSeedRebuildService")
public class AccountMasterSeedRebuildService extends AbstractFixedIntervalTransformationService
        implements TransformationService {

    private static final Log log = LogFactory.getLog(AccountMasterSeedRebuildService.class);

    private static final String VERSION = "VERSION";

    private static final String DATA_FLOW_BEAN_NAME = "accountMasterSeedRebuildFlow";

    @Autowired
    private AccountMasterSeed source;

    @Autowired
    private AccountMasterSeedRebuildDataFlowService transformationDataFlowService;
    
    @Override
    public boolean isManualTriggerred() {
        return true;
    }

    @Override
    public List<String> findUnprocessedVersions() {
        List<String> versions = new ArrayList<String>();
        versions.add(HdfsPathBuilder.dateFormat.format(new Date()));
        return versions;
    }

    @Override
    TransformationConfiguration createNewConfiguration(List<String> latestBaseVersion,
            String newLatestVersion, List<SourceColumn> sourceColumns) {
        AccountMasterSeedConfiguration configuration = new AccountMasterSeedConfiguration();
        setAdditionalDetails(newLatestVersion, sourceColumns, configuration);
        return configuration;
    }

    @Override
    Date checkTransformationConfigurationValidity(
            TransformationConfiguration transformationConfiguration) {
        AccountMasterSeedConfiguration conf = (AccountMasterSeedConfiguration) transformationConfiguration;
        conf.getSourceConfigurations().put(VERSION, conf.getVersion());
        try {
            return HdfsPathBuilder.dateFormat.parse(conf.getVersion());
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
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
        return Collections.emptyList();
    }

    @Override
    protected void executeDataFlow(TransformationProgress progress, String workflowDir,
            TransformationConfiguration transformationConfiguration) {
        transformationDataFlowService.executeDataProcessing(source, workflowDir, null,
                progress.getRootOperationUID(), DATA_FLOW_BEAN_NAME, transformationConfiguration);
    }

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    TransformationConfiguration readTransformationConfigurationObject(String confStr)
            throws IOException {
        return JsonUtils.deserialize(confStr, AccountMasterSeedConfiguration.class);
    }

    @Override
    public Class<? extends TransformationConfiguration> getConfigurationClass() {
        return AccountMasterSeedConfiguration.class;
    }

    @Override
    Log getLogger() {
        return log;
    }
}
