package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
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
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.HGDataCleanConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(HGDataCleanFlow.DATAFLOW_BEAN_NAME)
public class HGDataCleanFlow extends ConfigurableFlowBase<HGDataCleanConfig> {

    public static final String DATAFLOW_BEAN_NAME = "hgDataCleanFlow";
    public static final String TRANSFORMER_NAME = "hgDataCleanTransformer";

    private static final Long ONE_MONTH = TimeUnit.DAYS.toMillis(30);

    private HGDataCleanConfig config;

    private static final String ID = "_LDC_ID_";

    private static final String INTL_MAX_VERIFY_DATE = "MaxVerifyDate";
    private static final String INTL_MAX_INTENSITY = "MaxIntensity";
    private static final String INTL_LOCATION_COUNT = "LocationCount";
    private static final String INTL_DUMMY = "_LDC_DUMMY_";

    public static final String FINAL_DOMAIN = "Domain";
    public static final String FINAL_SUPPLIER_NAME = "Supplier_Name";
    public static final String FINAL_SEGMENT_NAME = "Segment_Name";
    public static final String FINAL_CAT1 = "HG_Category_1";
    public static final String FINAL_CAT2 = "HG_Category_2";
    public static final String FINAL_PARENT_CAT1 = "HG_Category_1_Parent";
    public static final String FINAL_PARENT_CAT2 = "HG_Category_2_Parent";
    public static final String FINAL_MAX_LOC_INTENSITY = "Max_Location_Intensity";
    public static final String FINAL_LOC_COUNT = "Location_Count";
    public static final String FINAL_LAST_VERIFIED_DATE = "Last_Verified_Date";
    public static final String FINAL_CREATION_DATE = "Creation_Date";
    public static final String FINAL_LAST_UPLOAD_DATE = "LE_Last_Upload_Date";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node source = addSource(parameters.getBaseTables().get(0));

        // Convert DateLastVerified from String to Long (Timestamp)
        source = source.apply(new DateToTimestampFunction(config.getDateLastVerifiedField()),
                new FieldList(config.getDateLastVerifiedField()),
                new FieldMetadata(config.getDateLastVerifiedField(), Long.class));
        // Convert Intensity from String to Integer
        source = source.apply(
                String.format("%s == null ? null : Integer.valueOf(%s)", config.getIntensityField(),
                        config.getIntensityField()),
                new FieldList(config.getIntensityField()),
                new FieldMetadata(config.getIntensityField(), Integer.class));
        // Add RowID
        source = source.addRowID(ID);
        
        // Find max DateLastVerified
        source = source.addColumnWithFixedValue(INTL_DUMMY, null, String.class);
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(config.getDateLastVerifiedField(), INTL_MAX_VERIFY_DATE, AggregationType.MAX));
        Node maxVerifyDate = source.groupBy(new FieldList(INTL_DUMMY), aggregations) //
                .retain(new FieldList(INTL_MAX_VERIFY_DATE, INTL_DUMMY)) //
                .renamePipe("max_verify_date");

        // Cleanup domain and remove rows without domain
        source = source.apply(new DomainCleanupFunction(config.getDomainField(), true),
                new FieldList(config.getDomainField()), new FieldMetadata(config.getDomainField(), String.class));

        // Find all the domain + techs with latest DateLastVerified
        FieldList domainTechFields = new FieldList(config.getDomainField(), config.getVendorField(), config.getProductField(),
                config.getCategoryField(), config.getCategory2Field(), config.getCategoryParentField(),
                config.getCategoryParent2Field());
        Node latest = source.groupByAndLimit(domainTechFields, new FieldList(config.getDateLastVerifiedField()), 1, true, false) //
                .retain(ID) //
                .renamePipe("latest");

        // Calculate Max_Location_Intensity and Location_Count per domain +
        // techs
        aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(config.getIntensityField(), INTL_MAX_INTENSITY, AggregationType.MAX));
        aggregations.add(new Aggregation(config.getDomainField(), INTL_LOCATION_COUNT, AggregationType.COUNT));
        Node aggregated = source.groupBy(domainTechFields, aggregations) //
                .apply(INTL_LOCATION_COUNT + ".intValue()", new FieldList(INTL_LOCATION_COUNT),
                        new FieldMetadata(INTL_LOCATION_COUNT, Integer.class));

        // Consolidate
        source = source.innerJoin(ID, latest, ID) //
                .innerJoin(INTL_DUMMY, maxVerifyDate, INTL_DUMMY)
                .innerJoin(domainTechFields, aggregated, domainTechFields)
                .retain(new FieldList(config.getDomainField(), config.getVendorField(), config.getProductField(),
                        config.getCategoryField(), config.getCategory2Field(), config.getCategoryParentField(),
                        config.getCategoryParent2Field(), config.getDateLastVerifiedField(), INTL_MAX_INTENSITY,
                        INTL_LOCATION_COUNT, INTL_MAX_VERIFY_DATE));

        // Add Creation_Date and LE_Last_Upload_Date
        Date now = new Date();
        Date fakedCurrentDate = config.getFakedCurrentDate();
        if (fakedCurrentDate != null) {
            now = fakedCurrentDate;
        }
        source = source.addTimestamp(FINAL_CREATION_DATE, now) //
                .addTimestamp(FINAL_LAST_UPLOAD_DATE, now);

        // Finalize schema
        source = source.rename(
                new FieldList(config.getDomainField(), config.getVendorField(), config.getProductField(),
                        config.getCategoryField(), config.getCategory2Field(),
                        config.getCategoryParentField(), config.getCategoryParent2Field(), INTL_MAX_INTENSITY,
                        INTL_LOCATION_COUNT, config.getDateLastVerifiedField()),
                new FieldList(FINAL_DOMAIN, FINAL_SUPPLIER_NAME, FINAL_SEGMENT_NAME, FINAL_CAT1, FINAL_CAT2,
                        FINAL_PARENT_CAT1, FINAL_PARENT_CAT2, FINAL_MAX_LOC_INTENSITY, FINAL_LOC_COUNT,
                        FINAL_LAST_VERIFIED_DATE));

        // Cleanup records if Last_Verified_Date is 6-month older than maximum
        // Last_Verified_Date
        source = source
                .filter(FINAL_LAST_VERIFIED_DATE + " + " + ONE_MONTH * 6 + "L >= " + INTL_MAX_VERIFY_DATE,
                        new FieldList(FINAL_LAST_VERIFIED_DATE, INTL_MAX_VERIFY_DATE)) //
                .discard(INTL_MAX_VERIFY_DATE);

        return source;
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
