package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ComputeRootDunsAndTypeFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.FormDomOwnershipTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

import cascading.tuple.Fields;

@Component(CleanupOrbSecSrcFlow.DATAFLOW_BEAN_NAME)
public class CleanupOrbSecSrcFlow extends ConfigurableFlowBase<FormDomOwnershipTableConfig> {
    public final static String DATAFLOW_BEAN_NAME = "CleanupOrbSecSrcFlow";
    public final static String TRANSFORMER_NAME = "CleanupOrbSecSrcTransformer";
    private final static String DUNS_TYPE = "DUNS_TYPE";
    private final static String REASON_TYPE = "REASON_TYPE";
    private final static String HIGHER_SALES_VOLUME = "HIGHER_SALES_VOLUME";
    private final static String HIGHER_EMP_TOTAL = "HIGHER_EMP_TOTAL";
    private final static String HIGHER_NUM_OF_LOC = "HIGHER_NUM_OF_LOC";
    private final static String ROOT_DUNS = "ROOT_DUNS";
    private final static String ORB_SEC_PRI_DOMAIN = "PrimaryDomain";
    private final static String ORB_SRC_SEC_DOMAIN = "SecondaryDomain";

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
        return FormDomOwnershipTableConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node domOwnershipTable = addSource(parameters.getBaseTables().get(0));
        Node orbSecSrc = addSource(parameters.getBaseTables().get(1));
        Node amSeed = addSource(parameters.getBaseTables().get(2));
        String checkNullDomainDuns = String.join(" && ", DataCloudConstants.AMS_ATTR_DUNS + " != null",
                "!" + DataCloudConstants.AMS_ATTR_DUNS + ".equals(\"\")",
                DataCloudConstants.AMS_ATTR_DOMAIN + " != null",
                "!" + DataCloudConstants.AMS_ATTR_DOMAIN + ".equals(\"\")");
        Node amSeedFiltered = amSeed //
                .filter(checkNullDomainDuns,
                        new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS));
        String filterDomOwnTable = String.join(" || ", REASON_TYPE + ".equals(\"" + HIGHER_SALES_VOLUME + "\")",
                REASON_TYPE + ".equals(\"" + HIGHER_EMP_TOTAL + "\")",
                REASON_TYPE + ".equals(\"" + HIGHER_NUM_OF_LOC + "\")");
        // retaining required columns : domain, root duns, dunsType
        Node domOwnTableFilter = domOwnershipTable //
                .filter(filterDomOwnTable, new FieldList(REASON_TYPE)) //
                .rename(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS),
                        new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN), renameField(ROOT_DUNS))); //
        Node orbSecSrcJoinAmSeed = orbSecSrc //
                .join(ORB_SEC_PRI_DOMAIN, amSeedFiltered, DataCloudConstants.AMS_ATTR_DOMAIN, JoinType.LEFT) //
                .retain(new FieldList(ORB_SEC_PRI_DOMAIN, ORB_SRC_SEC_DOMAIN, DataCloudConstants.ATTR_GU_DUNS,
                        DataCloudConstants.ATTR_DU_DUNS, DataCloudConstants.AMS_ATTR_DUNS));

        // compute ROOT_DUNS for orbSecSrc
        orbSecSrcJoinAmSeed = orbSecSrcJoinAmSeed //
                .addColumnWithFixedValue(ROOT_DUNS, null, String.class);

        Node orbSecSrcCleanedUp = computeRootDunsAndCompareToRetain(orbSecSrcJoinAmSeed, domOwnTableFilter);
        return orbSecSrcCleanedUp;
    }

    private static Node computeRootDunsAndCompareToRetain(Node orbSecSrcJoinAmSeed, Node domOwnTableFilter) {
        // add ROOT_DUNS to amSeedFiltered
        ComputeRootDunsAndTypeFunction computeRootDuns = new ComputeRootDunsAndTypeFunction(
                new Fields(orbSecSrcJoinAmSeed.getFieldNamesArray()), DataCloudConstants.ATTR_GU_DUNS,
                DataCloudConstants.ATTR_DU_DUNS, DataCloudConstants.AMS_ATTR_DUNS, ROOT_DUNS, DUNS_TYPE);

        Node orbSecSrcWithRootDuns = orbSecSrcJoinAmSeed //
                .apply(computeRootDuns, new FieldList(orbSecSrcJoinAmSeed.getFieldNames()),
                        orbSecSrcJoinAmSeed.getSchema(), new FieldList(orbSecSrcJoinAmSeed.getFieldNames()),
                        Fields.REPLACE);

        String domOwnTabRootDunsNotNull = renameField(ROOT_DUNS) + " != null";
        // filter ROOT_DUNS = null from DomainOwnershipTable
        Node filterDomOwnTabWithRootDuns = domOwnTableFilter //
                .filter(domOwnTabRootDunsNotNull, new FieldList(renameField(ROOT_DUNS)));

        // orbSecSrc leftJoin domOwnTable
        String filterNullDomOwnTab = renameField(DataCloudConstants.AMS_ATTR_DOMAIN) + " == null";
        Node orbJoinDomOwnTable = orbSecSrcWithRootDuns //
                .join(ORB_SRC_SEC_DOMAIN, filterDomOwnTabWithRootDuns, renameField(DataCloudConstants.AMS_ATTR_DOMAIN),
                        JoinType.LEFT);

        Node nonMatchOrbSecSrc = orbJoinDomOwnTable //
                .filter(filterNullDomOwnTab, new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN))) //
                .retain(new FieldList(ORB_SEC_PRI_DOMAIN, ORB_SRC_SEC_DOMAIN));
        String filterNotNullDomOwnTab = renameField(DataCloudConstants.AMS_ATTR_DOMAIN) + " != null";
        Node matchOrbSecSrc = orbJoinDomOwnTable //
                .filter(filterNotNullDomOwnTab, new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN)));
        // check if ROOT_DUNS of orbSecSrc and domOwnTable are same
        String orbSecSrcDunsNotNull = ROOT_DUNS + " != null";
        Node filterMatchedOrbWithRootDuns = matchOrbSecSrc //
                .filter(orbSecSrcDunsNotNull, new FieldList(ROOT_DUNS));
        String rootDunsEqual = ROOT_DUNS + ".equals(" + renameField(ROOT_DUNS) + ")";
        Node retainedOrbSecSrc = filterMatchedOrbWithRootDuns //
                .filter(rootDunsEqual, new FieldList(ROOT_DUNS, renameField(ROOT_DUNS))) //
                .retain(new FieldList(ORB_SEC_PRI_DOMAIN, ORB_SRC_SEC_DOMAIN));
        Node orbSecSrcCleanedUp = retainedOrbSecSrc //
                .merge(nonMatchOrbSecSrc);
        return orbSecSrcCleanedUp;
    }

    private static String renameField(String field) {
        return "renamed_" + field;
    }
}
