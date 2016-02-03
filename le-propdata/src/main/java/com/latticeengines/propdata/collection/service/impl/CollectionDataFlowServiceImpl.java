package com.latticeengines.propdata.collection.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.MostRecentDataFlowParameters;
import com.latticeengines.domain.exposed.propdata.PivotDataFlowParameters;
import com.latticeengines.propdata.collection.entitymanager.SourceColumnEntityMgr;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.CollectionDataFlowService;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.MostRecentSource;
import com.latticeengines.propdata.core.source.PivotedSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.HGData;
import com.latticeengines.propdata.core.source.impl.OrbIntelligenceMostRecent;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("collectionDataFlowService")
public class CollectionDataFlowServiceImpl implements CollectionDataFlowService {

    @Autowired
    protected DataTransformationService dataTransformationService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Autowired
    private OrbIntelligenceMostRecent orbIntelligenceMostRecent;

    @Autowired
    private HGData hgData;

    @Value("${propdata.collection.cascading.platform:tez}")
    protected String cascadingPlatform;

    @Override
    public void executeMergeRawData(MostRecentSource source, String uid) {
        executeMergeRawData(source, uid, "mostRecentFlow");
    }

    @Override
    public void executePivotData(PivotedSource source, String baseVersion, String uid, String flowBean) {
        String flowName = CollectionDataFlowKeys.PIVOT_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();

        Map<String, Table> sources = new HashMap<>();
        List<String> baseTables = new ArrayList<>();
        String[] versions = baseVersion.split("\\|");
        int i = 0;
        for (Source baseSource : source.getBaseSources()) {
            String version = versions[i];
            Table baseTable = hdfsSourceEntityMgr.getTableAtVersion(baseSource, version);
            sources.put(baseSource.getSourceName(), baseTable);
            baseTables.add(baseSource.getSourceName());
            i++;
        }

        PivotDataFlowParameters parameters = new PivotDataFlowParameters();
        parameters.setTimestampField(source.getTimestampField());
        parameters.setColumns(sourceColumnEntityMgr.getSourceColumns(source));
        parameters.setBaseTables(baseTables);
        parameters.setJoinFields(source.getPrimaryKey());

        DataFlowContext ctx = dataFlowContext(source, sources, parameters, targetPath);
        ctx.setProperty("FLOWNAME", source.getSourceName() + "-" + flowName);
        if (StringUtils.isEmpty(flowBean)) {
            flowBean = "pivotFlow";
        }
        dataTransformationService.executeNamedTransformation(ctx, flowBean);
    }

    @Override
    public void executeRefreshOrbIntelligence(String uid) {
        executeMergeRawData(orbIntelligenceMostRecent, uid, "orbIntelligenceRefreshFlow");
    }

    @Override
    public void executeRefreshHGData(String baseVersion, String uid) {
        String flowName = CollectionDataFlowKeys.TRANSFORM_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(hgData, flowName).append(uid).toString();

        Table baseTable = hdfsSourceEntityMgr.getTableAtVersion(hgData.getBaseSources()[0], baseVersion);
        Map<String, Table> sources = new HashMap<>();
        sources.put("Source", baseTable);

        DataFlowContext ctx = dataFlowContext(hgData, sources, new DataFlowParameters(), targetPath);
        ctx.setProperty("FLOWNAME", hgData.getSourceName() + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, "hgDataRefreshFlow");
    }

    public void executeMergeRawData(MostRecentSource source, String uid, String dataFlowBean) {
        String flowName = CollectionDataFlowKeys.MOST_RECENT_FLOW;

        CollectedSource baseSource = source.getBaseSources()[0];
        Date latestToArchive = hdfsSourceEntityMgr.getLatestTimestamp(baseSource);
        Date earliestToArchive = new Date(latestToArchive.getTime() - source.periodToKeep());

        Map<String, Table> sources = new HashMap<>();
        Table table = hdfsSourceEntityMgr.getCollectedTableSince(baseSource, earliestToArchive);
        sources.put(baseSource.getSourceName(), table);

        MostRecentDataFlowParameters parameters = new MostRecentDataFlowParameters();
        parameters.setDomainField(((DomainBased) source).getDomainField());
        parameters.setTimestampField(source.getTimestampField());
        parameters.setGroupbyFields(source.getPrimaryKey());
        parameters.setSourceTable(baseSource.getSourceName());
        parameters.setEarliest(earliestToArchive);

        String outputDir = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();
        DataFlowContext ctx = dataFlowContext(source, sources, parameters, outputDir);
        ctx.setProperty("FLOWNAME", source.getSourceName() + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, dataFlowBean);
    }

    private DataFlowContext dataFlowContext(Source source, Map<String, Table> sources, DataFlowParameters parameters,
            String outputDir) {
        String sourceName = source.getSourceName();
        DataFlowContext ctx = new DataFlowContext();
        if ("mr".equalsIgnoreCase(cascadingPlatform)) {
            ctx.setProperty("ENGINE", "MR");
        } else {
            ctx.setProperty("ENGINE", "TEZ");
        }

        ctx.setProperty("PARAMETERS", parameters);
        ctx.setProperty("SOURCETABLES", sources);
        ctx.setProperty("CUSTOMER", sourceName);
        ctx.setProperty("RECORDNAME", sourceName);
        ctx.setProperty("TARGETTABLENAME", sourceName);
        ctx.setProperty("TARGETPATH", outputDir);

        ctx.setProperty("QUEUE", LedpQueueAssigner.getPropDataQueueNameForSubmission());
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("JOBPROPERTIES", getJobProperties());
        return ctx;
    }

    protected Properties getJobProperties() {
        Properties jobProperties = new Properties();
        jobProperties.put("mapred.mapper.new-api", "false");
        return jobProperties;
    }

}
