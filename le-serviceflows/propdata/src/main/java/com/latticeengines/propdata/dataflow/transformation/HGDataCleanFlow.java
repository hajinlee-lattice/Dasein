package com.latticeengines.propdata.dataflow.transformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.DateToTimestampFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainCleanupFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.HGDataCleanConfiguration;

@Component("hgDataCleanFlow")
public class HGDataCleanFlow extends TransformationFlowBase<HGDataCleanConfiguration> {

    private static final String domainField = "URL";
    private static final Long ONE_MONTH = TimeUnit.DAYS.toMillis(30);

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return HGDataCleanConfiguration.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node source = addSource("Source");
        source = source.apply(new DomainCleanupFunction(domainField), new FieldList(domainField),
                new FieldMetadata(domainField, String.class));

        source = source.apply(new DateToTimestampFunction("DateLastVerified"), new FieldList("DateLastVerified"),
                new FieldMetadata("DateLastVerified", Long.class));

        FieldList contents = new FieldList("URL", "SupplierName", "ProductName", "HGCategory1", "HGCategory2",
                "HGCategory1Parent", "HGCategory2Parent");

        FieldList contentsWithDate = contents.addAll(Collections.singletonList("DateLastVerified"));

        Node latest = source.groupByAndLimit(contents, new FieldList("DateLastVerified"), 1, true, true);
        latest = latest.retain(contentsWithDate);
        latest = latest.renamePipe("latest");

        source = source.innerJoin(contentsWithDate, latest, contentsWithDate);

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("Intensity", "MaxIntensity", AggregationType.MAX));
        aggregations.add(new Aggregation("URL", "LocationCount", AggregationType.COUNT));
        Node aggregated = source.groupBy(contentsWithDate, aggregations);
        aggregated = aggregated.retain(new FieldList("URL", "SupplierName", "ProductName", "HGCategory1", "HGCategory2",
                "HGCategory1Parent", "HGCategory2Parent", "DateLastVerified", "MaxIntensity", "LocationCount"));

        aggregated = aggregated.addFunction("LocationCount.intValue()", new FieldList("LocationCount"),
                new FieldMetadata("LocationCount", Integer.class));

        aggregated = aggregated.addTimestamp("Creation_Date");
        aggregated = aggregated.addTimestamp("LE_Last_Upload_Date");

        aggregated = aggregated.rename(
                new FieldList("URL", "SupplierName", "ProductName", "HGCategory1", "HGCategory2", "HGCategory1Parent",
                        "HGCategory2Parent", "MaxIntensity", "LocationCount", "DateLastVerified"),
                new FieldList("Domain", "Supplier_Name", "Segment_Name", "HG_Category_1", "HG_Category_2",
                        "HG_Category_1_Parent", "HG_Category_2_Parent", "Max_Location_Intensity", "Location_Count",
                        "Last_Verified_Date"));

        aggregated = aggregated.filter("Last_Verified_Date + " + ONE_MONTH * 6 + "L >= LE_Last_Upload_Date",
                new FieldList("Last_Verified_Date", "LE_Last_Upload_Date"));

        return aggregated;
    }

}
