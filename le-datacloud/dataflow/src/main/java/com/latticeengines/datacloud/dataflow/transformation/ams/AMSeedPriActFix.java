package com.latticeengines.datacloud.dataflow.transformation.ams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ams.AMSeedPriDomainAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.ams.AMSeedPriLocAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.operations.OperationCode;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;
import com.latticeengines.domain.exposed.dataflow.operations.OperationMessage;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

@Component(AMSeedPriActFix.DATAFLOW_BEAN_NAME)
public class AMSeedPriActFix extends ConfigurableFlowBase<TransformerConfig> {

    public static final String DATAFLOW_BEAN_NAME = "AMSeedPriActFix";
    public static final String TRANSFORMER_NAME = DataCloudConstants.TRANSFORMER_AMSEED_PRIACT_FIX;

    private static final String LATTICEID_PRILOC = "_LatticeID_PriLoc_";
    private static final String LATTICEID_PRILOC_CTRY = "_LatticeID_PriLoc_Ctry_";
    private static final String LATTICEID_PRILOC_ST = "_LatticeID_PriLoc_St_";
    private static final String LATTICEID_PRILOC_ZIP = "_LatticeID_PriLoc_Zip_";
    private static final String LATTICEID_PRIDOM = "_LatticeID_PriDom_";

    private static final String PRILOC_LOG_FIELD = "PriLoc_" + OperationLogUtils.DEFAULT_FIELD_NAME;
    private static final String PRILOC_CTRY_LOG_FIELD = "PriLocCtry_" + OperationLogUtils.DEFAULT_FIELD_NAME;
    private static final String PRILOC_ST_LOG_FIELD = "PriLocSt_" + OperationLogUtils.DEFAULT_FIELD_NAME;
    private static final String PRILOC_ZIP_LOG_FIELD = "PriLocZip_" + OperationLogUtils.DEFAULT_FIELD_NAME;
    private static final String PRIDOM_LOG_FIELD = "PriDom_" + OperationLogUtils.DEFAULT_FIELD_NAME;


    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node ams = addSource(parameters.getBaseTables().get(0));
        ams = fixPriLoc(ams);
        ams = fixPriDom(ams);
        ams = appendOptLogs(ams);
        return ams;
    }

    @SuppressWarnings("rawtypes")
    private Node fixPriLoc(Node ams) {
        Node aggNode = ams.getSchema(OperationLogUtils.DEFAULT_FIELD_NAME) == null ? ams
                : ams.discard(new FieldList(OperationLogUtils.DEFAULT_FIELD_NAME));

        List<FieldMetadata> fms = new ArrayList<>(Arrays.asList(new FieldMetadata(LATTICEID_PRILOC, Long.class)));
        List<String> groupFields = new ArrayList<>(Arrays.asList(DataCloudConstants.AMS_ATTR_DOMAIN));
        Aggregator agg = new AMSeedPriLocAggregator(new Fields(LATTICEID_PRILOC), groupFields, LATTICEID_PRILOC);
        Node priLoc = aggNode.groupByAndAggregate(new FieldList(groupFields), agg, fms, true) //
                .rename(new FieldList(OperationLogUtils.DEFAULT_FIELD_NAME), new FieldList(PRILOC_LOG_FIELD)) //
                .renamePipe("PriLoc");

        fms = new ArrayList<>(Arrays.asList(new FieldMetadata(LATTICEID_PRILOC_CTRY, Long.class)));
        groupFields = new ArrayList<>(
                Arrays.asList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_COUNTRY));
        agg = new AMSeedPriLocAggregator(new Fields(LATTICEID_PRILOC_CTRY), groupFields, LATTICEID_PRILOC_CTRY);
        Node ctryPriLoc = aggNode.groupByAndAggregate(new FieldList(groupFields), agg, fms, true) //
                .rename(new FieldList(OperationLogUtils.DEFAULT_FIELD_NAME), new FieldList(PRILOC_CTRY_LOG_FIELD)) //
                .renamePipe("CtryPriLoc");

        fms = new ArrayList<>(Arrays.asList(new FieldMetadata(LATTICEID_PRILOC_ST, Long.class)));
        groupFields = new ArrayList<>(Arrays.asList(DataCloudConstants.AMS_ATTR_DOMAIN,
                DataCloudConstants.AMS_ATTR_COUNTRY, DataCloudConstants.AMS_ATTR_STATE));
        agg = new AMSeedPriLocAggregator(new Fields(LATTICEID_PRILOC_ST), groupFields, LATTICEID_PRILOC_ST);
        Node stPriLoc = aggNode.groupByAndAggregate(new FieldList(groupFields), agg, fms, true) //
                .rename(new FieldList(OperationLogUtils.DEFAULT_FIELD_NAME), new FieldList(PRILOC_ST_LOG_FIELD)) //
                .renamePipe("StPriLoc");

        fms = new ArrayList<>(Arrays.asList(new FieldMetadata(LATTICEID_PRILOC_ZIP, Long.class)));
        groupFields = new ArrayList<>(
                Arrays.asList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_COUNTRY,
                        DataCloudConstants.AMS_ATTR_ZIP));
        agg = new AMSeedPriLocAggregator(new Fields(LATTICEID_PRILOC_ZIP), groupFields, LATTICEID_PRILOC_ZIP);
        Node zipPriLoc = aggNode.groupByAndAggregate(new FieldList(groupFields), agg, fms, true) //
                .rename(new FieldList(OperationLogUtils.DEFAULT_FIELD_NAME), new FieldList(PRILOC_ZIP_LOG_FIELD)) //
                .renamePipe("ZipPriLoc");

        ams = ams.join(new FieldList(DataCloudConstants.LATTICE_ID), priLoc, new FieldList(LATTICEID_PRILOC), JoinType.LEFT)
                .join(new FieldList(DataCloudConstants.LATTICE_ID), ctryPriLoc, new FieldList(LATTICEID_PRILOC_CTRY),
                        JoinType.LEFT)
                .join(new FieldList(DataCloudConstants.LATTICE_ID), stPriLoc, new FieldList(LATTICEID_PRILOC_ST),
                        JoinType.LEFT)
                .join(new FieldList(DataCloudConstants.LATTICE_ID), zipPriLoc, new FieldList(LATTICEID_PRILOC_ZIP),
                        JoinType.LEFT);
        ams = ams.discard(new FieldList(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION))
                .apply(String.format("(%s == null || %s != null) ? \"Y\" : \"N\"", DataCloudConstants.AMS_ATTR_DOMAIN,
                        LATTICEID_PRILOC), new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, LATTICEID_PRILOC),
                        new FieldMetadata(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION, String.class))
                .apply(String.format("%s != null ? \"Y\" : \"N\"", LATTICEID_PRILOC_CTRY),
                        new FieldList(LATTICEID_PRILOC_CTRY),
                        new FieldMetadata(DataCloudConstants.ATTR_IS_CTRY_PRIMARY_LOCATION, String.class))
                .apply(String.format("%s != null ? \"Y\" : \"N\"", LATTICEID_PRILOC_ST),
                        new FieldList(LATTICEID_PRILOC_ST),
                        new FieldMetadata(DataCloudConstants.ATTR_IS_ST_PRIMARY_LOCATION, String.class))
                .apply(String.format("%s != null ? \"Y\" : \"N\"", LATTICEID_PRILOC_ZIP),
                        new FieldList(LATTICEID_PRILOC_ZIP),
                        new FieldMetadata(DataCloudConstants.ATTR_IS_ZIP_PRIMARY_LOCATION, String.class))
                .discard(new FieldList(LATTICEID_PRILOC, LATTICEID_PRILOC_CTRY, LATTICEID_PRILOC_ST,
                        LATTICEID_PRILOC_ZIP));

        return ams;
    }

    @SuppressWarnings("rawtypes")
    private Node fixPriDom(Node ams) {
        Node aggNode = ams.getSchema(OperationLogUtils.DEFAULT_FIELD_NAME) == null ? ams
                : ams.discard(new FieldList(OperationLogUtils.DEFAULT_FIELD_NAME));

        List<FieldMetadata> fms = new ArrayList<>(Arrays.asList(new FieldMetadata(LATTICEID_PRIDOM, Long.class)));
        List<String> groupFields = new ArrayList<>(Arrays.asList(DataCloudConstants.AMS_ATTR_DUNS));
        Aggregator agg = new AMSeedPriDomainAggregator(new Fields(LATTICEID_PRIDOM), groupFields, LATTICEID_PRIDOM);
        Node priDom = aggNode.groupByAndAggregate(new FieldList(groupFields), agg, fms, true) //
                .rename(new FieldList(OperationLogUtils.DEFAULT_FIELD_NAME), new FieldList(PRIDOM_LOG_FIELD)) //
                .renamePipe("PriDom");

        ams = ams.join(new FieldList(DataCloudConstants.LATTICE_ID), priDom, new FieldList(LATTICEID_PRIDOM),
                JoinType.LEFT);

        ams = ams
                // Original LE_IS_PRIMARY_DOMAIN is renamed to
                // LE_IS_PRIMARY_DOMAIN_tmp
                .rename(new FieldList(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN),
                        new FieldList(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN + "_tmp")) //
                // (DUNS == null || _LatticeID_PriDom_ != null) ? "Y" : "N"
                .apply(String.format("(%s == null || %s != null) ? \"Y\" : \"N\"", DataCloudConstants.AMS_ATTR_DUNS,
                        LATTICEID_PRIDOM), new FieldList(DataCloudConstants.AMS_ATTR_DUNS, LATTICEID_PRIDOM),
                        new FieldMetadata(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN, String.class)) //
                // (LE_IS_PRIMARY_DOMAIN.equals("N") && !LE_IS_PRIMARY_DOMAIN.equals(LE_IS_PRIMARY_DOMAIN_tmp))
                // ? "[Step=AMSeedPriActFixTransformer,Code=NOT_PRIMARY_DOMAIN,Log=Not manual seed primary account]"
                // : PriDom_LE_OperationLog
                .apply(String.format("%s.equals(\"N\") && !%s.equals(%s) ? \"%s\" : %s",
                        DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN, DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN,
                        DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN + "_tmp",
                        OperationLogUtils.buildLog(TRANSFORMER_NAME, OperationCode.NOT_PRI_DOM,
                                OperationMessage.NON_PRIMARY_ACCOUNT),
                        PRIDOM_LOG_FIELD),
                        new FieldList(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN,
                                DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN + "_tmp", PRIDOM_LOG_FIELD),
                        new FieldMetadata(PRIDOM_LOG_FIELD + "_tmp", String.class)) //
                .discard(new FieldList(LATTICEID_PRIDOM, PRIDOM_LOG_FIELD)) //
                .rename(new FieldList(PRIDOM_LOG_FIELD + "_tmp"), new FieldList(PRIDOM_LOG_FIELD));

        return ams;
    }

    private Node appendOptLogs(Node ams) {
        return ams.appendOptLogFromField(PRILOC_LOG_FIELD) //
                .appendOptLogFromField(PRILOC_CTRY_LOG_FIELD) //
                .appendOptLogFromField(PRILOC_ST_LOG_FIELD) //
                .appendOptLogFromField(PRILOC_ZIP_LOG_FIELD) //
                .appendOptLogFromField(PRIDOM_LOG_FIELD) //
                .discard(new FieldList(PRILOC_LOG_FIELD, PRILOC_CTRY_LOG_FIELD, PRILOC_ST_LOG_FIELD,
                        PRILOC_ZIP_LOG_FIELD, PRIDOM_LOG_FIELD));
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
