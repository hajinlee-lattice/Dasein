package com.latticeengines.propdata.dataflow.transformation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.AddNullColumns;
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
        
        Calendar cal = Calendar.getInstance();
        cal.setTime(parameters.getTimestamp());
        cal.add(Calendar.DATE, -8);
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String cufOffDate = dateFormat.format(cal.getTime());
        bombora7Days = bombora7Days.filter("UniversalDateTime.compareToIgnoreCase(\"" + cufOffDate + "\") >= 0",
                new FieldList("UniversalDateTime"));

        bombora7Days = bombora7Days.addRowID("ID");
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("SourceID", "ContentSources", AggregationType.COUNT));
        aggregations.add(new Aggregation("HashedEmailIDBase64", "UniqueUsers", AggregationType.COUNT));
        aggregations.add(new Aggregation("ID", "TotalViews", AggregationType.COUNT));
        aggregations.add(new Aggregation("TopicScore", "TotalAggregatedScore", AggregationType.SUM));
        bombora7Days = bombora7Days.groupBy(new FieldList("Domain", "Topic", "PostalCode"), aggregations);
        bombora7Days = bombora7Days.retain(new FieldList("Domain", "Topic", "PostalCode", "ContentSources",
                "TotalViews", "UniqueUsers", "TotalAggregatedScore"));
        bombora7Days = bombora7Days.groupByAndLimit(new FieldList("Domain", "Topic"),
                new FieldList("TotalAggregatedScore"), 1, true, true);
        bombora7Days = bombora7Days.rename(new FieldList("PostalCode"),
                new FieldList("ZipCodeOfHighestAggregatedScore"));

        bombora7Days = bombora7Days.addFunction("Long.valueOf(ContentSources).intValue()",
                new FieldList("ContentSources"),
                new FieldMetadata("ContentSources", Integer.class));
        bombora7Days = bombora7Days.addFunction("Long.valueOf(UniqueUsers).intValue()", new FieldList("UniqueUsers"),
                new FieldMetadata("UniqueUsers", Integer.class));
        bombora7Days = bombora7Days.addFunction("Long.valueOf(TotalViews).intValue()", new FieldList("TotalViews"),
                new FieldMetadata("TotalViews", Integer.class));
        bombora7Days = bombora7Days.addFunction("Double.valueOf(TotalAggregatedScore).floatValue()",
                new FieldList("TotalAggregatedScore"),
                new FieldMetadata("TotalAggregatedScore", Float.class));

        bombora7Days = bombora7Days.apply(
                new AddNullColumns(new Fields("HighlyRelevantSources", "MostRelevantSources",
                        "TotalAggregatedScore_HighlyRelevant", "TotalAggregatedScore_MostRelevant")),
                new FieldList("Domain"),
                Arrays.asList(
                        new FieldMetadata("HighlyRelevantSources", Integer.class),
                        new FieldMetadata("MostRelevantSources", Integer.class),
                        new FieldMetadata("TotalAggregatedScore_HighlyRelevant", Float.class),
                        new FieldMetadata("TotalAggregatedScore_MostRelevant", Float.class)),
                new FieldList(
                        "Domain", "Topic", "ZipCodeOfHighestAggregatedScore", "ContentSources",
                        "TotalViews", "UniqueUsers", "TotalAggregatedScore",
                        "HighlyRelevantSources", "MostRelevantSources",
                        "TotalAggregatedScore_HighlyRelevant", "TotalAggregatedScore_MostRelevant"));

        bombora7Days.addTimestamp("Date", parameters.getTimestamp());
        return bombora7Days;
    }
}
