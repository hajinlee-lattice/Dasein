package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterReduceParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.ReducedAccountMaster;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

@Component("accountMasterReduceDataFlowService")
public class AccountMasterReduceDataFlowService extends AbstractTransformationDataFlowService {

    private static final Log log = LogFactory.getLog(AccountMasterReduceDataFlowService.class);

    public void executeDataProcessing(Source source, String workflowDir, String sourceVersion, String uid,
            String dataFlowBean, TransformationConfiguration transformationConfiguration) {

        if (StringUtils.isEmpty(dataFlowBean)) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(), "Name of dataFlowBean cannot be null" });
        }

        ReducedAccountMaster reducedAccountMaster = (ReducedAccountMaster) source;
        String flowName = CollectionDataFlowKeys.TRANSFORM_FLOW;
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
        ctx.setProperty(DataFlowProperty.FLOWNAME, source.getSourceName() + HIPHEN + flowName);

        ctx.setProperty(DataFlowProperty.SOURCETABLES, sourceTables);

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

}
