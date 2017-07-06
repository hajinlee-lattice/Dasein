package com.latticeengines.datacloud.collection.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.service.CollectionDataFlowKeysDeprecated;
import com.latticeengines.datacloud.collection.service.CollectionDataFlowService;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.MostRecentSource;
import com.latticeengines.datacloud.core.source.PivotedSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.HGData;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.MostRecentDataFlowParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.PivotDataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@SuppressWarnings("deprecation")
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
    private HGData hgData;

    @Value("${datacloud.collection.cascading.platform}")
    protected String cascadingPlatform;

    @Value("${datacloud.collection.cascading.partitions}")
    protected Integer cascadingPartitions;

    @Override
    public void executeMergeRawData(MostRecentSource source, String uid, String dataFlowBean) {
        if (StringUtils.isEmpty(dataFlowBean)) {
            dataFlowBean = "mostRecentFlow";
        }

        String flowName = CollectionDataFlowKeysDeprecated.MOST_RECENT_FLOW;

        CollectedSource baseSource = (CollectedSource) source.getBaseSources()[0];
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
        ctx.setProperty(DataFlowProperty.FLOWNAME, source.getSourceName() + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, dataFlowBean);
    }

    @Override
    public void executePivotData(PivotedSource source, String baseVersion, String uid, String flowBean) {
        String flowName = CollectionDataFlowKeysDeprecated.PIVOT_FLOW;
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
        parameters.setColumns(sourceColumnEntityMgr.getSourceColumns(source.getSourceName()));
        parameters.setBaseTables(baseTables);
        parameters.setJoinFields(source.getPrimaryKey());
        parameters.setHasSqlPresence(source instanceof HasSqlPresence);

        DataFlowContext ctx = dataFlowContext(source, sources, parameters, targetPath);
        ctx.setProperty(DataFlowProperty.FLOWNAME, source.getSourceName() + "-" + flowName);
        if (StringUtils.isEmpty(flowBean)) {
            flowBean = "pivotFlow";
        }
        dataTransformationService.executeNamedTransformation(ctx, flowBean);
    }

    @Override
    public void executeRefreshHGData(String baseVersion, String uid) {
        String flowName = CollectionDataFlowKeysDeprecated.TRANSFORM_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(hgData, flowName).append(uid).toString();

        Table baseTable = hdfsSourceEntityMgr.getTableAtVersion(hgData.getBaseSources()[0], baseVersion);
        Map<String, Table> sources = new HashMap<>();
        sources.put("Source", baseTable);

        DataFlowContext ctx = dataFlowContext(hgData, sources, new DataFlowParameters(), targetPath);
        ctx.setProperty(DataFlowProperty.FLOWNAME, hgData.getSourceName() + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, "hgDataRefreshFlow");
    }

    private DataFlowContext dataFlowContext(Source source, Map<String, Table> sources, DataFlowParameters parameters,
            String outputDir) {
        String sourceName = source.getSourceName();
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty(DataFlowProperty.ENGINE, cascadingPlatform.toUpperCase());
        ctx.setProperty(DataFlowProperty.PARAMETERS, parameters);
        ctx.setProperty(DataFlowProperty.SOURCETABLES, sources);
        ctx.setProperty(DataFlowProperty.CUSTOMER, sourceName);
        ctx.setProperty(DataFlowProperty.RECORDNAME, sourceName);
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, sourceName);
        ctx.setProperty(DataFlowProperty.TARGETPATH, outputDir);

        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getPropDataQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.CHECKPOINT, false);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, yarnConfiguration);
        ctx.setProperty(DataFlowProperty.PARTITIONS, cascadingPartitions);
        ctx.setProperty(DataFlowProperty.JOBPROPERTIES, new Properties());
        return ctx;
    }

}
