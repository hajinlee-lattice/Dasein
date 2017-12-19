package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMSeedPriLocAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

@Component(AMSeedPriActFix.DATAFLOW_BEAN_NAME)
public class AMSeedPriActFix extends ConfigurableFlowBase<TransformerConfig> {

    public static final String DATAFLOW_BEAN_NAME = "AMSeedPriActFix";
    public static final String TRANSFORMER_NAME = "AMSeedPriActFixTransformer";

    private static final String LATTICEID_PRILOC = "_LatticeID_PriLoc_";
    private static final String LATTICEID_PRILOC_CTY = "_LatticeID_PriLoc_Cty_";
    private static final String LATTICEID_PRILOC_ST = "_LatticeID_PriLoc_St_";
    private static final String LATTICEID_PRILOC_ZIP = "_LatticeID_PriLoc_Zip_";

    @SuppressWarnings("rawtypes")
    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node ams = addSource(parameters.getBaseTables().get(0));

        List<FieldMetadata> fms = new ArrayList<>(Arrays.asList(new FieldMetadata(LATTICEID_PRILOC, Long.class)));
        List<String> groupFields = new ArrayList<>(Arrays.asList(DataCloudConstants.AMS_ATTR_DOMAIN));
        Aggregator agg = new AMSeedPriLocAggregator(new Fields(LATTICEID_PRILOC), groupFields);
        Node priLoc = ams.groupByAndAggregate(new FieldList(groupFields), agg, fms).renamePipe("PriLoc");

        fms = new ArrayList<>(Arrays.asList(new FieldMetadata(LATTICEID_PRILOC_CTY, Long.class)));
        groupFields = new ArrayList<>(
                Arrays.asList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_COUNTRY));
        agg = new AMSeedPriLocAggregator(new Fields(LATTICEID_PRILOC_CTY), groupFields);
        Node ctyPriLoc = ams.groupByAndAggregate(new FieldList(groupFields), agg, fms)
                .renamePipe("CtyPriLoc");

        fms = new ArrayList<>(Arrays.asList(new FieldMetadata(LATTICEID_PRILOC_ST, Long.class)));
        groupFields = new ArrayList<>(Arrays.asList(DataCloudConstants.AMS_ATTR_DOMAIN,
                DataCloudConstants.AMS_ATTR_COUNTRY, DataCloudConstants.AMS_ATTR_STATE));
        agg = new AMSeedPriLocAggregator(new Fields(LATTICEID_PRILOC_ST), groupFields);
        Node stPriLoc = ams.groupByAndAggregate(new FieldList(groupFields), agg, fms)
                .renamePipe("StPriLoc");

        fms = new ArrayList<>(Arrays.asList(new FieldMetadata(LATTICEID_PRILOC_ZIP, Long.class)));
        groupFields = new ArrayList<>(
                Arrays.asList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_COUNTRY,
                        DataCloudConstants.AMS_ATTR_ZIP));
        agg = new AMSeedPriLocAggregator(new Fields(LATTICEID_PRILOC_ZIP), groupFields);
        Node zipPriLoc = ams.groupByAndAggregate(new FieldList(groupFields), agg, fms)
                .renamePipe("PriLocPerDomCtyZip");

        ams = ams.join(new FieldList(DataCloudConstants.LATTIC_ID), priLoc, new FieldList(LATTICEID_PRILOC), JoinType.LEFT)
                 .join(new FieldList(DataCloudConstants.LATTIC_ID), ctyPriLoc, new FieldList(LATTICEID_PRILOC_CTY), JoinType.LEFT)
                 .join(new FieldList(DataCloudConstants.LATTIC_ID), stPriLoc, new FieldList(LATTICEID_PRILOC_ST), JoinType.LEFT)
                .join(new FieldList(DataCloudConstants.LATTIC_ID), zipPriLoc, new FieldList(LATTICEID_PRILOC_ZIP),
                        JoinType.LEFT);
        ams = ams.discard(new FieldList(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION))
                .apply(String.format("(%s == null || %s != null) ? \"Y\" : \"N\"", DataCloudConstants.AMS_ATTR_DOMAIN,
                        LATTICEID_PRILOC), new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, LATTICEID_PRILOC),
                        new FieldMetadata(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION, String.class))
                .apply(String.format("%s != null ? \"Y\" : \"N\"", LATTICEID_PRILOC_CTY),
                        new FieldList(LATTICEID_PRILOC_CTY),
                        new FieldMetadata(DataCloudConstants.ATTR_IS_CTY_PRIMARY_LOCATION, String.class))
                .apply(String.format("%s != null ? \"Y\" : \"N\"", LATTICEID_PRILOC_ST),
                        new FieldList(LATTICEID_PRILOC_ST),
                        new FieldMetadata(DataCloudConstants.ATTR_IS_ST_PRIMARY_LOCATION, String.class))
                .apply(String.format("%s != null ? \"Y\" : \"N\"", LATTICEID_PRILOC_ZIP),
                        new FieldList(LATTICEID_PRILOC_ZIP),
                        new FieldMetadata(DataCloudConstants.ATTR_IS_ZIP_PRIMARY_LOCATION, String.class))
                .discard(new FieldList(LATTICEID_PRILOC, LATTICEID_PRILOC_CTY, LATTICEID_PRILOC_ST,
                        LATTICEID_PRILOC_ZIP));

        return ams;
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
        return TransformerConfig.class;
    }

}
