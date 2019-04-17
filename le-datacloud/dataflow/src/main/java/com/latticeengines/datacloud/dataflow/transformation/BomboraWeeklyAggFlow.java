package com.latticeengines.datacloud.dataflow.transformation;

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
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("bomboraWeeklyAggFlow")
public class BomboraWeeklyAggFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, TransformationFlowParameters> {

    private final static String ID = "ID";
    private final static String DOMAIN = "Domain";
    private final static String TOTAL_VIEWS = "TotalViews";
    private final static String TOPIC_SCORE = "TopicScore";
    private final static String TOTAL_AGGREGATED_SCORE = "TotalAggregatedScore";
    private final static String TOPIC = "Topic";
    private final static String POSTAL_CODE = "PostalCode";
    private final static String CONTENT_SOURCES = "ContentSources";
    private final static String UNIQUE_USERS = "UniqueUsers";
    private final static String ZIP_CODE_OF_HIGHEST_AGGREGATED_SCORE = "ZipCodeOfHighestAggregatedScore";
    private final static String HIGHLY_RELEVANT_SOURCES = "HighlyRelevantSources";
    private final static String MOST_RELEVANT_SOURCES = "MostRelevantSources";
    private final static String TOTAL_AGGREGATED_SCORE_HIGHLY_RELEVANT = "TotalAggregatedScore_HighlyRelevant";
    private final static String TOTAL_AGGREGATED_SCORE_MOST_RELEVANT = "TotalAggregatedScore_MostRelevant";
    private final static String SOURCE_ID = "SourceID";
    private final static String HASHED_EMAIL_ID = "HashedEmailIDBase64";
    private final static String DATE = "Date";


    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node bombora7Days = addSource(parameters.getBaseTables().get(0));
        bombora7Days = bombora7Days.addRowID(ID);

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(ID, TOTAL_VIEWS, AggregationType.COUNT));
        aggregations.add(new Aggregation(TOPIC_SCORE, TOTAL_AGGREGATED_SCORE, AggregationType.SUM));
        Node bomboraWeeklyAgg = bombora7Days.groupBy(new FieldList(DOMAIN, TOPIC, POSTAL_CODE), aggregations);
        bomboraWeeklyAgg = bomboraWeeklyAgg
                .retain(new FieldList(DOMAIN, TOPIC, POSTAL_CODE, TOTAL_VIEWS, TOTAL_AGGREGATED_SCORE));
        bomboraWeeklyAgg = bomboraWeeklyAgg.groupByAndLimit(new FieldList(DOMAIN, TOPIC),
                new FieldList(TOTAL_AGGREGATED_SCORE), 1, true, true);

        List<FieldMetadata> bomboraWeeklyAggMetadata = new ArrayList<FieldMetadata>();
        bomboraWeeklyAggMetadata.add(new FieldMetadata(DOMAIN, String.class));
        bomboraWeeklyAggMetadata.add(new FieldMetadata(TOPIC, String.class));
        bomboraWeeklyAggMetadata.add(new FieldMetadata(POSTAL_CODE, String.class));
        bomboraWeeklyAggMetadata.add(new FieldMetadata(TOTAL_VIEWS, Long.class));
        bomboraWeeklyAggMetadata.add(new FieldMetadata(TOTAL_AGGREGATED_SCORE, Double.class));
        bomboraWeeklyAgg.setSchema(bomboraWeeklyAggMetadata);

        bombora7Days = bombora7Days.renamePipe("BomboraDepivoted7Days");
        bombora7Days = renameBomboraDepivotedColumn(bombora7Days);

        bomboraWeeklyAgg = bomboraWeeklyAgg.join(new FieldList(DOMAIN, TOPIC, POSTAL_CODE), bombora7Days,
                new FieldList("Daily_" + DOMAIN, "Daily_" + TOPIC, "Daily_" + POSTAL_CODE), JoinType.INNER);

        List<FieldMetadata> bomboraWeeklyAggMetadataUpdated = new ArrayList<FieldMetadata>();
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata(DOMAIN, String.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata(TOPIC, String.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata(POSTAL_CODE, String.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata(TOTAL_VIEWS, Long.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata(TOTAL_AGGREGATED_SCORE, Double.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata(CONTENT_SOURCES, Integer.class));
        bomboraWeeklyAggMetadataUpdated.add(new FieldMetadata(UNIQUE_USERS, Integer.class));

        Fields bomboraWeeklyAggFields = new Fields(DOMAIN, TOPIC, POSTAL_CODE, TOTAL_VIEWS, TOTAL_AGGREGATED_SCORE,
                CONTENT_SOURCES, UNIQUE_USERS);
        bomboraWeeklyAgg = bomboraWeeklyAgg.groupByAndBuffer(
                new FieldList(DOMAIN, TOPIC, POSTAL_CODE, TOTAL_VIEWS, TOTAL_AGGREGATED_SCORE),
                new BomboraWeeklyAggBuffer(bomboraWeeklyAggFields, CONTENT_SOURCES, "Daily_" + SOURCE_ID, UNIQUE_USERS,
                        "Daily_" + HASHED_EMAIL_ID),
                bomboraWeeklyAggMetadataUpdated);


        bomboraWeeklyAgg = bomboraWeeklyAgg.rename(new FieldList(POSTAL_CODE),
                new FieldList(ZIP_CODE_OF_HIGHEST_AGGREGATED_SCORE));
        bomboraWeeklyAgg = bomboraWeeklyAgg.apply("Long.valueOf(" + TOTAL_VIEWS + ").intValue()",
                new FieldList(TOTAL_VIEWS), new FieldMetadata(TOTAL_VIEWS, Integer.class));
        bomboraWeeklyAgg = bomboraWeeklyAgg.apply("Double.valueOf(" + TOTAL_AGGREGATED_SCORE + ").floatValue()",
                new FieldList(TOTAL_AGGREGATED_SCORE), new FieldMetadata(TOTAL_AGGREGATED_SCORE, Float.class));

        bomboraWeeklyAgg = bomboraWeeklyAgg.apply(
                new AddNullColumns(new Fields(HIGHLY_RELEVANT_SOURCES, MOST_RELEVANT_SOURCES,
                        TOTAL_AGGREGATED_SCORE_HIGHLY_RELEVANT, TOTAL_AGGREGATED_SCORE_MOST_RELEVANT)),
                new FieldList(DOMAIN),
                Arrays.asList(new FieldMetadata(HIGHLY_RELEVANT_SOURCES, Integer.class),
                        new FieldMetadata(MOST_RELEVANT_SOURCES, Integer.class),
                        new FieldMetadata(TOTAL_AGGREGATED_SCORE_HIGHLY_RELEVANT, Float.class),
                        new FieldMetadata(TOTAL_AGGREGATED_SCORE_MOST_RELEVANT, Float.class)),
                new FieldList(DOMAIN, TOPIC, ZIP_CODE_OF_HIGHEST_AGGREGATED_SCORE, CONTENT_SOURCES, TOTAL_VIEWS,
                        UNIQUE_USERS, TOTAL_AGGREGATED_SCORE, HIGHLY_RELEVANT_SOURCES, MOST_RELEVANT_SOURCES,
                        TOTAL_AGGREGATED_SCORE_HIGHLY_RELEVANT, TOTAL_AGGREGATED_SCORE_MOST_RELEVANT));

        bomboraWeeklyAgg = bomboraWeeklyAgg.addTimestamp(DATE, parameters.getTimestamp());
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
