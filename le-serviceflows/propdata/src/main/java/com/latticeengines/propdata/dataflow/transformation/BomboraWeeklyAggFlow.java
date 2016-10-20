package com.latticeengines.propdata.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.AddNullColumns;
import com.latticeengines.dataflow.runtime.cascading.propdata.BomboraWeeklyAggBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BasicTransformationConfiguration;

import cascading.tuple.Fields;

@Component("bomboraWeeklyAggFlow")
public class BomboraWeeklyAggFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, TransformationFlowParameters> {

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node bombora7Days = addSource(parameters.getBaseTables().get(0));
        bombora7Days = bombora7Days.addRowID("ID");

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("ID", "TotalViews", AggregationType.COUNT));
        aggregations.add(new Aggregation("TopicScore", "TotalAggregatedScore", AggregationType.SUM));
        Node bomboraWeeklyAgg = bombora7Days.groupBy(new FieldList("Domain", "Topic", "PostalCode"), aggregations);
        bomboraWeeklyAgg = bomboraWeeklyAgg
                .retain(new FieldList("Domain", "Topic", "PostalCode", "TotalViews", "TotalAggregatedScore"));
        bomboraWeeklyAgg = bomboraWeeklyAgg.groupByAndLimit(new FieldList("Domain", "Topic"),
                new FieldList("TotalAggregatedScore"), 1, true, true);

        List<FieldMetadata> bomboraWeeklyAggMetadata = new ArrayList<FieldMetadata>();
        bomboraWeeklyAggMetadata.add(new FieldMetadata("Domain", String.class));
        bomboraWeeklyAggMetadata.add(new FieldMetadata("Topic", String.class));
        bomboraWeeklyAggMetadata.add(new FieldMetadata("PostalCode", String.class));
        bomboraWeeklyAggMetadata.add(new FieldMetadata("TotalViews", Long.class));
        bomboraWeeklyAggMetadata.add(new FieldMetadata("TotalAggregatedScore", Double.class));
        bomboraWeeklyAgg.setSchema(bomboraWeeklyAggMetadata);

        bombora7Days = bombora7Days.renamePipe("BomboraDepivoted7Days");
        bombora7Days = renameBomboraDepivotedColumn(bombora7Days);

        bomboraWeeklyAgg = bomboraWeeklyAgg.join(new FieldList("Domain", "Topic", "PostalCode"), bombora7Days,
                new FieldList("Daily_Domain", "Daily_Topic", "Daily_PostalCode"), JoinType.INNER);

        List<FieldMetadata> bomboraWeeklyAggMetadataUpdated = new ArrayList<FieldMetadata>();
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata("Domain", String.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata("Topic", String.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata("PostalCode", String.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata("TotalViews", Long.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata("TotalAggregatedScore", Double.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata("ContentSources", Integer.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata("UniqueUsers", Integer.class));

        Fields bomboraWeeklyAggFields = new Fields("Domain", "Topic", "PostalCode", "TotalViews",
                "TotalAggregatedScore", "ContentSources", "UniqueUsers");
        bomboraWeeklyAgg = bomboraWeeklyAgg.groupByAndBuffer(
                new FieldList("Domain", "Topic", "PostalCode", "TotalViews", "TotalAggregatedScore"),
                new BomboraWeeklyAggBuffer(bomboraWeeklyAggFields, "ContentSources", "Daily_SourceID", "UniqueUsers",
                        "Daily_HashedEmailIDBase64"),
                bomboraWeeklyAggMetadataUpdated);


        bomboraWeeklyAgg = bomboraWeeklyAgg.rename(new FieldList("PostalCode"),
                new FieldList("ZipCodeOfHighestAggregatedScore"));
        bomboraWeeklyAgg = bomboraWeeklyAgg.addFunction("Long.valueOf(TotalViews).intValue()",
                new FieldList("TotalViews"), new FieldMetadata("TotalViews", Integer.class));
        bomboraWeeklyAgg = bomboraWeeklyAgg.addFunction("Double.valueOf(TotalAggregatedScore).floatValue()",
                new FieldList("TotalAggregatedScore"), new FieldMetadata("TotalAggregatedScore", Float.class));

        bomboraWeeklyAgg = bomboraWeeklyAgg.apply(
                new AddNullColumns(new Fields("HighlyRelevantSources", "MostRelevantSources",
                        "TotalAggregatedScore_HighlyRelevant", "TotalAggregatedScore_MostRelevant")),
                new FieldList("Domain"),
                Arrays.asList(new FieldMetadata("HighlyRelevantSources", Integer.class),
                        new FieldMetadata("MostRelevantSources", Integer.class),
                        new FieldMetadata("TotalAggregatedScore_HighlyRelevant", Float.class),
                        new FieldMetadata("TotalAggregatedScore_MostRelevant", Float.class)),
                new FieldList("Domain", "Topic", "ZipCodeOfHighestAggregatedScore", "ContentSources", "TotalViews",
                        "UniqueUsers", "TotalAggregatedScore", "HighlyRelevantSources", "MostRelevantSources",
                        "TotalAggregatedScore_HighlyRelevant", "TotalAggregatedScore_MostRelevant"));

        bomboraWeeklyAgg = bomboraWeeklyAgg.addTimestamp("Date", parameters.getTimestamp());
        return bomboraWeeklyAgg;
    }

    private Node renameBomboraDepivotedColumn(Node node) {
        List<String> newNames = new ArrayList<String>();
        List<String> oldNames = node.getFieldNames();
        for (String oldName : oldNames) {
            newNames.add("Daily_" + oldName);
        }
        return node.rename(new FieldList(oldNames), new FieldList(newNames));
    }
}
