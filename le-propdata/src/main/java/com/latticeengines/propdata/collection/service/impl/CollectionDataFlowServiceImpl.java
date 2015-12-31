package com.latticeengines.propdata.collection.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.collection.dataflow.merge.MergeDataFlowParameters;
import com.latticeengines.propdata.collection.dataflow.pivot.PivotDataFlowParameters;
import com.latticeengines.propdata.collection.entitymanager.HdfsSourceEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.SourceColumnEntityMgr;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.CollectionDataFlowService;
import com.latticeengines.propdata.collection.source.CollectedSource;
import com.latticeengines.propdata.collection.source.DomainBased;
import com.latticeengines.propdata.collection.source.MostRecentSource;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.Source;
import com.latticeengines.propdata.collection.source.impl.HGData;
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
    private HGData hgData;

    @Value("${propdata.collection.mapred.reduce.tasks:8}")
    private int reduceTasks;

    @Value("${propdata.collection.cascading.platform:tez}")
    protected String cascadingPlatform;

    @Override
    public void executeMergeRawData(MostRecentSource source, String uid) {
        String flowName = CollectionDataFlowKeys.MERGE_RAW_FLOW;

        CollectedSource baseSource = source.getBaseSources()[0];
        Map<String, Table> sources = new HashMap<>();
        Table table = hdfsSourceEntityMgr.getTableAtVersion(baseSource, null);
        sources.put(baseSource.getSourceName(), table);

        MergeDataFlowParameters parameters = new MergeDataFlowParameters();
        parameters.setDomainField(((DomainBased) source).getDomainField());
        parameters.setTimestampField(source.getTimestampField());
        parameters.setGroupbyFields(source.getPrimaryKey());
        parameters.setSourceTable(baseSource.getSourceName());

        String outputDir = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();
        DataFlowContext ctx = dataFlowContext(source, sources, parameters, outputDir);
        ctx.setProperty("FLOWNAME", source.getSourceName() + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, "mergeRawFlow");
    }

    @Override
    public void executePivotData(PivotedSource source, String baseVersion, String uid) {
        String flowName = CollectionDataFlowKeys.PIVOT_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();

        Map<String, Table> sources = new HashMap<>();
        List<String> baseTables = new ArrayList<>();
        for (Source baseSource: source.getBaseSources()) {
            Table baseTable = hdfsSourceEntityMgr.getTableAtVersion(baseSource, baseVersion);
            sources.put(baseSource.getSourceName(), baseTable);
            baseTables.add(baseSource.getSourceName());
        }

        PivotDataFlowParameters parameters = new PivotDataFlowParameters();
        parameters.setTimestampField(source.getTimestampField());
        parameters.setColumns(sourceColumnEntityMgr.getConfigurableColumns(source));
        parameters.setBaseTables(baseTables);
        parameters.setJoinFields(source.getPrimaryKey());

        DataFlowContext ctx = dataFlowContext(source, sources, parameters, targetPath);
        ctx.setProperty("FLOWNAME", source.getSourceName() + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, "pivotBaseSource");
    }

    @Override
    public void executeRefreshHGData(String baseVersion, String uid) {
        String flowName = CollectionDataFlowKeys.TRANSFORM_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(hgData, flowName).append(uid).toString();

        Table baseTable = hdfsSourceEntityMgr.getTableAtVersion(hgData.getBaseSources()[0], baseVersion);
        Map<String, Table> sources = new HashMap<>();
        sources.put(CollectionDataFlowKeys.SOURCE, baseTable);

        DataFlowContext ctx = dataFlowContext(hgData, sources, new DataFlowParameters(), targetPath);
        ctx.setProperty("FLOWNAME", hgData.getSourceName() + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, "hgDataRefreshFlow");
    }

    private DataFlowContext dataFlowContext(Source source,
                                            Map<String, Table> sources,
                                            DataFlowParameters parameters,
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
