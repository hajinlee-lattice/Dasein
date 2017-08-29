package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.HGDataCleanConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(HGDataCleanFlow.DATAFLOW_BEAN_NAME)
public class HGDataCleanFlow extends ConfigurableFlowBase<HGDataCleanConfig> {

    public static final String DATAFLOW_BEAN_NAME = "hgDataCleanFlow";
    public static final String TRANSFORMER_NAME = "hgDataCleanTransformer";

    private static final Long ONE_MONTH = TimeUnit.DAYS.toMillis(30);

    private HGDataCleanConfig config;

    private static final String ID = "_LDC_ID_";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node source = addSource(parameters.getBaseTables().get(0));
        source = source.apply(new DomainCleanupFunction(config.getDomainField(), true),
                new FieldList(config.getDomainField()), new FieldMetadata(config.getDomainField(), String.class));

        source = source.apply(new DateToTimestampFunction(config.getDateLastVerifiedField()),
                new FieldList(config.getDateLastVerifiedField()),
                new FieldMetadata(config.getDateLastVerifiedField(), Long.class));

        source = source.addRowID(ID);
        
        FieldList contents = new FieldList(config.getDomainField(), config.getVendorField(), config.getProductField(),
                config.getCategoryField(), config.getCategory2Field(), config.getCategoryParentField(),
                config.getCategoryParent2Field());

        FieldList contentsWithDate = contents.addAll(Collections.singletonList(config.getDateLastVerifiedField()));

        Node latest = source.groupByAndLimit(contents, new FieldList(config.getDateLastVerifiedField()), 1, true,
                false);
        latest = latest.retain(ID);
        latest = latest.renamePipe("latest");

        source = source.innerJoin(ID, latest, ID);

        
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(config.getIntensityField(), "MaxIntensity", AggregationType.MAX));
        aggregations.add(new Aggregation(config.getDomainField(), "LocationCount", AggregationType.COUNT));
        Node aggregated = source.groupBy(contentsWithDate, aggregations);
        aggregated = aggregated.retain(new FieldList(config.getDomainField(), config.getVendorField(),
                config.getProductField(), config.getCategoryField(), config.getCategory2Field(),
                config.getCategoryParentField(), config.getCategoryParent2Field(), config.getDateLastVerifiedField(),
                "MaxIntensity", "LocationCount"));

        aggregated = aggregated.apply("LocationCount.intValue()", new FieldList("LocationCount"),
                new FieldMetadata("LocationCount", Integer.class));

        Date now = new Date();
        Date fakedCurrentDate = config.getFakedCurrentDate();
        if (fakedCurrentDate != null) {
            now = fakedCurrentDate;
        }

        aggregated = aggregated.addTimestamp("Creation_Date", now);
        aggregated = aggregated.addTimestamp("LE_Last_Upload_Date", now);

        aggregated = aggregated.rename(
                new FieldList(config.getDomainField(), config.getVendorField(), config.getProductField(),
                        config.getCategoryField(), config.getCategory2Field(),
                        config.getCategoryParentField(), config.getCategoryParent2Field(), "MaxIntensity",
                        "LocationCount", config.getDateLastVerifiedField()),
                new FieldList("Domain", "Supplier_Name", "Segment_Name", "HG_Category_1", "HG_Category_2",
                        "HG_Category_1_Parent", "HG_Category_2_Parent", "Max_Location_Intensity", "Location_Count",
                        "Last_Verified_Date"));

        aggregated = aggregated.filter("Last_Verified_Date + " + ONE_MONTH * 6 + "L >= LE_Last_Upload_Date",
                new FieldList("Last_Verified_Date", "LE_Last_Upload_Date"));

        return aggregated;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return HGDataCleanConfig.class;
    }

}
