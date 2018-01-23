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

@Component(CleanupAmSeedSrcFlow.DATAFLOW_BEAN_NAME)
public class CleanupAmSeedSrcFlow extends ConfigurableFlowBase<FormDomOwnershipTableConfig> {
    public final static String DATAFLOW_BEAN_NAME = "CleanupAmSeedSrcFlow";
    public final static String TRANSFORMER_NAME = "CleanupAmSeedSrcTransformer";
    private final static String REASON_TYPE = "REASON_TYPE";
    private final static String HIGHER_SALES_VOLUME = "HIGHER_SALES_VOLUME";
    private final static String HIGHER_EMP_TOTAL = "HIGHER_EMP_TOTAL";
    private final static String HIGHER_NUM_OF_LOC = "HIGHER_NUM_OF_LOC";
    private final static String ROOT_DUNS = "ROOT_DUNS";
    private final static String DUNS_TYPE = "DUNS_TYPE";
    private final static String IS_RETAINED = "IS_RETAINED";
    private final static String RETAIN_YES = "YES";
    private final static String RETAIN_NO = "NO";
    private final static String NULL_DOMAIN = "NULL_DOMAIN";

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
        Node amSeed = addSource(parameters.getBaseTables().get(1));
        // filter null domain and duns from amSeed
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
        Node domOwnTableForCleanup = domOwnershipTable //
                .filter(filterDomOwnTable, new FieldList(REASON_TYPE)) //
                .rename(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS),
                        new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN), renameField(ROOT_DUNS))) //
                .retain(new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN), renameField(ROOT_DUNS),
                        DUNS_TYPE));

        amSeedFiltered = amSeedFiltered //
                .addColumnWithFixedValue(ROOT_DUNS, null, String.class);

        Node finalCleanedupAmSeed = computeRootDunsAndCompare(amSeedFiltered, domOwnTableForCleanup,
                checkNullDomainDuns);

        String filterNullDomain = DataCloudConstants.AMS_ATTR_DOMAIN + " == null && " + DataCloudConstants.AMS_ATTR_DUNS
                + " != null";
        String filterDomDunsExist = DataCloudConstants.AMS_ATTR_DOMAIN + " != null && "
                + DataCloudConstants.AMS_ATTR_DUNS + " != null";
        // remove duns only entries if another record exists with the value of
        // same duns
        Node cleanedUpAmSeedWithNullDom = finalCleanedupAmSeed //
                .filter(filterNullDomain,
                        new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS)) //
                .renamePipe("cleanedUpAmSeedWithNullDom");
        Node cleanedUpAmSeedWithDom = finalCleanedupAmSeed //
                .filter(filterDomDunsExist,
                        new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS));

        Node renamedCleanedUpAmSeedWithDom = cleanedUpAmSeedWithDom //
                .rename(new FieldList(DataCloudConstants.AMS_ATTR_DUNS),
                        new FieldList(renameField(DataCloudConstants.AMS_ATTR_DUNS))) //
                .retain(new FieldList(DataCloudConstants.ATTR_DU_DUNS, renameField(DataCloudConstants.AMS_ATTR_DUNS),
                        DataCloudConstants.ATTR_EMPLOYEE_TOTAL, DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS,
                        DataCloudConstants.ATTR_SALES_VOL_US, DataCloudConstants.AMS_ATTR_DOMAIN,
                        DataCloudConstants.ATTR_GU_DUNS, ROOT_DUNS)) //
                .renamePipe("cleanedUpAmSeedWithDom");
        String filterRecordsToCleanup = renameField(DataCloudConstants.AMS_ATTR_DUNS) + " == null";
        // to check the DUNS that persist with domain as well
        Node dunsOnlyEntriesCleanedup = cleanedUpAmSeedWithNullDom //
                .join(DataCloudConstants.AMS_ATTR_DUNS, renamedCleanedUpAmSeedWithDom,
                        renameField(DataCloudConstants.AMS_ATTR_DUNS), JoinType.LEFT) //
                .filter(filterRecordsToCleanup, new FieldList(renameField(DataCloudConstants.AMS_ATTR_DUNS))) //
                .retain(new FieldList(amSeed.getFieldNames()));
        cleanedUpAmSeedWithDom = cleanedUpAmSeedWithDom //
                .retain(new FieldList(amSeed.getFieldNames()));
        Node finalCleanedupResult = cleanedUpAmSeedWithDom //
                .merge(dunsOnlyEntriesCleanedup);
        return finalCleanedupResult;
    }

    private static Node computeRootDunsAndCompare(Node amSeedFiltered, Node domOwnTableForCleanup,
            String checkNullDomainDuns) {
        // add ROOT_DUNS to amSeedFiltered
        ComputeRootDunsAndTypeFunction computeRootDuns = new ComputeRootDunsAndTypeFunction(
                new Fields(amSeedFiltered.getFieldNamesArray()), DataCloudConstants.ATTR_GU_DUNS,
                DataCloudConstants.ATTR_DU_DUNS, DataCloudConstants.AMS_ATTR_DUNS, ROOT_DUNS, DUNS_TYPE);

        Node amSeedWithRootDuns = amSeedFiltered //
                .apply(computeRootDuns, new FieldList(amSeedFiltered.getFieldNames()), amSeedFiltered.getSchema(),
                        new FieldList(amSeedFiltered.getFieldNames()), Fields.REPLACE);

        String amSeedRootDunsNotNull = ROOT_DUNS + " != null";
        String domOwnTabRootDunsNotNull = renameField(ROOT_DUNS) + " != null";

        // filter ROOT_DUNS = null from DomainOwnershipTable and AMSeedFiltered
        Node filterAmSeedWithRootDuns = amSeedWithRootDuns //
                .filter(amSeedRootDunsNotNull, new FieldList(ROOT_DUNS));
        Node filterDomOwnTabWithRootDuns = domOwnTableForCleanup //
                .filter(domOwnTabRootDunsNotNull, new FieldList(renameField(ROOT_DUNS)));

        // amSeed left join domainOwnTable
        Node joinAmSeedWithDomOwnTable = filterAmSeedWithRootDuns //
                .join(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN), filterDomOwnTabWithRootDuns,
                        new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN)), JoinType.LEFT);

        // Not in domOwnTable are : having TREE_NUMBER = 1 and ones that are not
        // to be cleaned up (like FRANCHISE/MULTIPLE_LARGE_COMPANY)
        String filterDomainNull = renameField(DataCloudConstants.AMS_ATTR_DOMAIN) + " == null";
        Node recNotInDomOwnTable = joinAmSeedWithDomOwnTable //
                .filter(filterDomainNull, new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN))) //
                .retain(new FieldList(amSeedFiltered.getFieldNames()));

        String filterDomainNotNull = renameField(DataCloudConstants.AMS_ATTR_DOMAIN) + " != null";
        Node recInDomOwnTable = joinAmSeedWithDomOwnTable //
                .filter(filterDomainNotNull, new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN)));

        // expression function to check if ROOT_DUNS of amSeed and domOwnTable
        // are same : if equal then
        // mark RETAIN=YES and if not equal then mark RETAIN=NO
        String rootDunsEqual = ROOT_DUNS + ".equals(" + renameField(ROOT_DUNS) + ")";
        String rootDunsNotEqual = "!" + ROOT_DUNS + ".equals(" + renameField(ROOT_DUNS) + ")";
        Node chkRecMatchInDomOwnTab = recInDomOwnTable //
                .filter(rootDunsEqual, new FieldList(ROOT_DUNS, renameField(ROOT_DUNS))) //
                .addColumnWithFixedValue(IS_RETAINED, RETAIN_YES, String.class);

        Node chkRecNotMatchInDomOwnTab = recInDomOwnTable //
                .filter(rootDunsNotEqual, new FieldList(ROOT_DUNS, renameField(ROOT_DUNS))) //
                .addColumnWithFixedValue(IS_RETAINED, RETAIN_NO, String.class);

        Node amSeedWithRetain = chkRecMatchInDomOwnTab //
                .merge(chkRecNotMatchInDomOwnTab);

        String filterRetainPresent = IS_RETAINED + ".equals(\"" + RETAIN_YES + "\")";
        String filterNotRetain = IS_RETAINED + ".equals(\"" + RETAIN_NO + "\")";
        Node filterNotRetained = null;
        Node filterRetained = null;
        if (amSeedWithRetain != null) {
            filterNotRetained = amSeedWithRetain //
                    .filter(filterNotRetain, new FieldList(IS_RETAINED)) //
                    .addColumnWithFixedValue(NULL_DOMAIN, null, String.class) //
                    .rename(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, NULL_DOMAIN),
                            new FieldList(NULL_DOMAIN, DataCloudConstants.AMS_ATTR_DOMAIN)) //
                    .retain(new FieldList(amSeedFiltered.getFieldNames()));

            filterRetained = amSeedWithRetain //
                    .filter(filterRetainPresent, new FieldList(IS_RETAINED)) //
                    .retain(new FieldList(amSeedFiltered.getFieldNames()));
        }

        Node finalCleanedupAmSeed = filterNotRetained //
                .merge(filterRetained) //
                .merge(recNotInDomOwnTable);
        // filtering null domain & duns row introduced by cleanup process
        Node filteredNullFinalNode = finalCleanedupAmSeed //
                .filter(checkNullDomainDuns,
                        new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS));
        return filteredNullFinalNode;
    }

    private static String renameField(String field) {
        return "renamed_" + field;
    }

}
