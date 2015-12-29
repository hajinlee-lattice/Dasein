package com.latticeengines.propdata.collection.dataflow.refresh;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.propdata.collection.dataflow.function.DomainCleanupFunction;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;

@Component("hgDataRefreshFlow")
public class HGDataRefreshFlow extends TypesafeDataFlowBuilder<DataFlowParameters> {

    private static final String domainField = "URL";

    @Override
    public Node construct(DataFlowParameters parameters) {
        Node source = addSource(CollectionDataFlowKeys.SOURCE);
        source = source.apply(new DomainCleanupFunction(domainField), new FieldList(domainField),
                new FieldMetadata(domainField, String.class));
        source = source.addRowID("ID");

        FieldList contents = new FieldList("URL", "SupplierName", "ProductName", "HGCategory1", "HGCategory2",
                "HGCategory1Parent", "HGCategory2Parent");

        FieldList contentsWithDate = contents.addAll(Collections.singletonList("DateLastVerified"));

        Node latest = source.groupByAndLimit(contents, new FieldList("DateLastVerified"), 1, true, true);
        latest = latest.retain(contentsWithDate);
        latest = latest.renamePipe("latest");

        source = source.innerJoin(contentsWithDate, latest, contentsWithDate);

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("Intensity", "MaxIntensity", Aggregation.AggregationType.MAX));
        aggregations.add(new Aggregation("ID", "LocationCount", Aggregation.AggregationType.COUNT));
        Node aggregated = source.groupBy(contentsWithDate, aggregations);
        aggregated = aggregated.retain(new FieldList("URL", "SupplierName", "ProductName",
                "HGCategory1", "HGCategory2", "HGCategory1Parent", "HGCategory2Parent", "DateLastVerified",
                "MaxIntensity", "LocationCount"));

        aggregated.addTimestamp("Creation_Date");
        aggregated.addTimestamp("LE_Last_Upload_Date");

        aggregated = aggregated.rename(new FieldList(
                "URL",
                "SupplierName",
                "ProductName",
                "HGCategory1",
                "HGCategory2",
                "HGCategory1Parent",
                "HGCategory2Parent",
                "MaxIntensity",
                "LocationCount",
                "DateLastVerified"
        ), new FieldList(
                "Domain",
                "Supplier_Name",
                "Segment_Name",
                "HG_Category_1",
                "HG_Category_1_Parent",
                "HG_Category_2_Parent",
                "HG_Category_2",
                "Max_Location_Intensity",
                "Location_Count",
                "Last_Verified_Date"
        ));

        return aggregated;
    }

}