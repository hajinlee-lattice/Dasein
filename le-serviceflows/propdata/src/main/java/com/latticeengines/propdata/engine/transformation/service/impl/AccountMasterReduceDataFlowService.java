package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterReduceParameters;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.ReducedAccountMaster;
import com.latticeengines.propdata.engine.common.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

@Component("accountMasterReduceDataFlowService")
public class AccountMasterReduceDataFlowService extends AbstractTransformationDataFlowService {

    private static final Log log = LogFactory.getLog(AccountMasterReduceDataFlowService.class);

    private static final String SOURCETABLES = "SOURCETABLES";

    private static final String HIPHEN = "-";

    private static final String FLOWNAME = "FLOWNAME";

    @Autowired
    protected DataTransformationService dataTransformationService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Value("${propdata.collection.cascading.platform:tez}")
    protected String cascadingPlatform;

    @Override
    public void executeDataProcessing(Source source, String workflowDir, String sourceVersion, String uid,
            String dataFlowBean, TransformationConfiguration transformationConfiguration) {

        if (StringUtils.isEmpty(dataFlowBean)) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(), "Name of dataFlowBean cannot be null" });
        }

        ReducedAccountMaster reducedAccountMaster = (ReducedAccountMaster) source;
        String flowName = CollectionDataFlowKeys.REBUILD_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();

        log.info("Creating ReducedAccountMaster @" + sourceVersion);

        Map<String, Table> sourceTables = new HashMap<>();

        // Set up base sources.
        List<String> baseTables = new ArrayList<>();
        for (Source baseSource : reducedAccountMaster.getBaseSources()) {
            log.info("Add base source " + baseSource.getSourceName());
            addSource(sourceTables, baseSource);
            baseTables.add(baseSource.getSourceName());
        }

        AccountMasterReduceParameters parameters = new AccountMasterReduceParameters();

        parameters.setTimestamp(new Date());
        parameters.setTimestampField(reducedAccountMaster.getTimestampField());
        parameters.setBaseTables(baseTables);
        parameters.setHasSqlPresence(source instanceof HasSqlPresence);

        DataFlowContext ctx = dataFlowContext(source, sourceTables, parameters, targetPath);
        ctx.setProperty(FLOWNAME, source.getSourceName() + HIPHEN + flowName);

        ctx.setProperty(SOURCETABLES, sourceTables);

        dataTransformationService.executeNamedTransformation(ctx, dataFlowBean);
    }

    private boolean addSource(Map<String, Table> sourceTables, Source source) {

        String sourceName = source.getSourceName();
        log.info("Add source " + sourceName);

        Table sourceTable = null;
        try {
            String version = hdfsSourceEntityMgr.getCurrentVersion(source);
            sourceTable = hdfsSourceEntityMgr.getTableAtVersion(source, version);
            log.info("Select source " + sourceName + "@version " + version);


        } catch (Exception e) {
            log.info("Source " + sourceName + " is not initiated in HDFS");
            e.printStackTrace();
            return false;
        }

        sourceTables.put(sourceName, sourceTable);
        return true;
    }

    @Override
    Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    @Override
    String getCascadingPlatform() {
        return cascadingPlatform;
    }
}
