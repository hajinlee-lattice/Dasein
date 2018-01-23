package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ComputeRootDunsAndTypeFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainRowSelectorAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainTreeCountAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.FormDomOwnershipTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(FormDomainOwnershipTableFlow.DATAFLOW_BEAN_NAME)
public class FormDomainOwnershipTableFlow extends ConfigurableFlowBase<FormDomOwnershipTableConfig> {
    public final static String DATAFLOW_BEAN_NAME = "FormDomainOwnershipTableFlow";
    public final static String TRANSFORMER_NAME = "FormDomOwnershipTableTransformer";
    private final static String ROOT_DUNS = "ROOT_DUNS";
    private final static String DUNS_TYPE = "DUNS_TYPE";
    private final static String TREE_NUMBER = "TREE_NUMBER";
    private final static String REASON_TYPE = "REASON_TYPE";
    private final static String FRANCHISE = "FRANCHISE";
    private final static String NULL_ROOT_DUNS = "NULL_ROOT_DUNS";
    private final static String NULL_ROOT_TYPE = "NULL_ROOT_TYPE";
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
        FormDomOwnershipTableConfig config = getTransformerConfig(parameters);
        Node amSeed = addSource(parameters.getBaseTables().get(0));
        Node orbSecSrc = addSource(parameters.getBaseTables().get(1));
        // filter null duns from amSeed
        String checkNullDunsDomain = String.join(" && ", DataCloudConstants.AMS_ATTR_DUNS + " != null",
                "!" + DataCloudConstants.AMS_ATTR_DUNS + ".equals(\"\")",
                DataCloudConstants.AMS_ATTR_DOMAIN + " != null",
                "!" + DataCloudConstants.AMS_ATTR_DOMAIN + ".equals(\"\")");
        Node amSeedFilteredNull = amSeed //
                .filter(checkNullDunsDomain,
                        new FieldList(DataCloudConstants.AMS_ATTR_DUNS, DataCloudConstants.AMS_ATTR_DOMAIN));
        Node amSeedDomDuns = amSeedFilteredNull //
                .retain(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS)) //
                .renamePipe("amSeedDomDuns");
        // Join orbSecSrc with amSeed on domain to get other fields info
        Node orbSecJoinedResult = orbSecSrc //
                .join(ORB_SEC_PRI_DOMAIN, amSeedDomDuns, DataCloudConstants.AMS_ATTR_DOMAIN, JoinType.INNER) //
                .retain(new FieldList(ORB_SRC_SEC_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS)) //
                .rename(new FieldList(ORB_SRC_SEC_DOMAIN), new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN)) //
                .retain(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS));
        // merge nodes : orbSecSrc and amSeed, then dedup the result
        Node mergedDomDuns = amSeedDomDuns //
                .merge(orbSecJoinedResult) //
                .groupByAndLimit(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS),
                        1);
        // join merged list of domain DUNS with amSeed : to get required
        // columns(SalesVolume, totalEmp, numOfLocations)
        Node mergedDomDunsWithAmSeed = mergedDomDuns //
                .join(DataCloudConstants.AMS_ATTR_DUNS, amSeedFilteredNull, DataCloudConstants.AMS_ATTR_DUNS,
                        JoinType.INNER) //
                .retain(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS,
                        DataCloudConstants.ATTR_GU_DUNS,
                        DataCloudConstants.ATTR_DU_DUNS, DataCloudConstants.ATTR_SALES_VOL_US,
                        DataCloudConstants.ATTR_EMPLOYEE_TOTAL,
                        DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS));
        Node rootTypePopulated = filterAndSetRootDunsType(mergedDomDunsWithAmSeed, amSeed, config);
        Node domOwnershipTable = constructDomOwnershipTable(rootTypePopulated, config);
        return domOwnershipTable;
    }

    private static Node filterAndSetRootDunsType(Node mergedDomDunsWithAmSeed, Node amSeed,
            FormDomOwnershipTableConfig config) {
        String rootDunsFirmo = DataCloudConstants.AMS_ATTR_DUNS + ".equals(" + ROOT_DUNS + ")";

        // Adding the required columns
        mergedDomDunsWithAmSeed = mergedDomDunsWithAmSeed //
                .addColumnWithFixedValue(ROOT_DUNS, null, String.class) //
                .addColumnWithFixedValue(DUNS_TYPE, null, String.class);

        // Setting RootDuns and dunsType based on selection criteria
        ComputeRootDunsAndTypeFunction computeRootDuns = new ComputeRootDunsAndTypeFunction(
                new Fields(mergedDomDunsWithAmSeed.getFieldNamesArray()), DataCloudConstants.ATTR_GU_DUNS,
                DataCloudConstants.ATTR_DU_DUNS, DataCloudConstants.AMS_ATTR_DUNS, ROOT_DUNS, DUNS_TYPE);

        Node rootTypePopulated = mergedDomDunsWithAmSeed
                .apply(computeRootDuns, new FieldList(mergedDomDunsWithAmSeed.getFieldNames()),
                        mergedDomDunsWithAmSeed.getSchema(), new FieldList(mergedDomDunsWithAmSeed.getFieldNames()),
                        Fields.REPLACE);

        Node rootOfTrees = rootTypePopulated
                .filter(rootDunsFirmo, new FieldList(DataCloudConstants.AMS_ATTR_DUNS, ROOT_DUNS)) //
                .retain(new FieldList(ROOT_DUNS, DataCloudConstants.ATTR_SALES_VOL_US,
                        DataCloudConstants.ATTR_EMPLOYEE_TOTAL, DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS)) //
                .renamePipe("RootOfTrees");

        Node rootFirmoAdd = rootTypePopulated //
                .retain(DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS, DUNS_TYPE) //
                .join(ROOT_DUNS, rootOfTrees, ROOT_DUNS, JoinType.INNER) //
                .retain(DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS, DUNS_TYPE,
                        DataCloudConstants.ATTR_SALES_VOL_US, DataCloudConstants.ATTR_EMPLOYEE_TOTAL,
                        DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS);

        Node dedupRootFirmo = rootFirmoAdd //
                .groupByAndLimit(new FieldList(rootFirmoAdd.getFieldNames()), 1);
        return dedupRootFirmo;
    }

    private static Node constructDomOwnershipTable(Node rootTypePopulated, FormDomOwnershipTableConfig config) {
        List<FieldMetadata> fms = fieldMetadataPrep(config);
        DomainTreeCountAggregator agg = new DomainTreeCountAggregator(
                new Fields(DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS, DUNS_TYPE, TREE_NUMBER),
                DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS, DUNS_TYPE);
        Node domainTreeNumber = rootTypePopulated
                .groupByAndAggregate(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN), agg, fms) //
                .renamePipe("DomainTreeNumber");

        String chkFranchiseExp = TREE_NUMBER + " >= " + config.getFranchiseThreshold();
        // filtering franchise and setting its rootDuns and dunsType as Null
        Node franchiseSet = domainTreeNumber //
                .filter(chkFranchiseExp, new FieldList(TREE_NUMBER)) //
                .addColumnWithFixedValue(NULL_ROOT_DUNS, null, String.class) //
                .addColumnWithFixedValue(NULL_ROOT_TYPE, null, String.class) //
                .retain(DataCloudConstants.AMS_ATTR_DOMAIN, NULL_ROOT_DUNS, NULL_ROOT_TYPE, TREE_NUMBER) //
                .rename(new FieldList(NULL_ROOT_DUNS, NULL_ROOT_TYPE), new FieldList(ROOT_DUNS, DUNS_TYPE)) //
                .retain(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS, DUNS_TYPE, TREE_NUMBER)) //
                .addColumnWithFixedValue(REASON_TYPE, FRANCHISE, String.class);

        String chkTreeCount = TREE_NUMBER + " > 1 && " + TREE_NUMBER + " < " + config.getFranchiseThreshold();
        // filtering domains present in more that one trees
        Node domainInManyTrees = domainTreeNumber //
                .filter(chkTreeCount, new FieldList(TREE_NUMBER));
        // join domainTreeNumber with dedupedCombinedSet for remaining
        // columns to be populated
        Node rootDunsRetain = domainInManyTrees //
                .rename(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN),
                        new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN))) //
                .retain(new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN), TREE_NUMBER));
        Node allColCombined = rootDunsRetain //
                .join(renameField(DataCloudConstants.AMS_ATTR_DOMAIN), rootTypePopulated,
                        DataCloudConstants.AMS_ATTR_DOMAIN,
                        JoinType.INNER);

        List<FieldMetadata> fmsForDomSelect = fieldMetadataWithReason(config);

        // aggregator to choose domain from one of the trees
        DomainRowSelectorAggregator domainSelect = new DomainRowSelectorAggregator(
                new Fields(DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS, DUNS_TYPE, TREE_NUMBER,
                        REASON_TYPE),
                DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS, DUNS_TYPE,
                DataCloudConstants.ATTR_SALES_VOL_US, DataCloudConstants.ATTR_EMPLOYEE_TOTAL,
                DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS,
                TREE_NUMBER, REASON_TYPE,
                config.getMultLargeCompThreshold());
        Node domainRowSelect = allColCombined //
                .groupByAndAggregate(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN), domainSelect, fmsForDomSelect) //
                .retain(new FieldList(franchiseSet.getFieldNames())) //
                .renamePipe("DomainRowSelect");
        Node domOwnershipTable = franchiseSet //
                .merge(domainRowSelect);
        return domOwnershipTable;
    }

    private static String renameField(String field) {
        return "renamed_" + field;
    }

    private static List<FieldMetadata> fieldMetadataWithReason(FormDomOwnershipTableConfig config) {
        return Arrays.asList(new FieldMetadata(DataCloudConstants.AMS_ATTR_DOMAIN, String.class), //
                new FieldMetadata(ROOT_DUNS, String.class), //
                new FieldMetadata(DUNS_TYPE, String.class), //
                new FieldMetadata(TREE_NUMBER, Integer.class), //
                new FieldMetadata(REASON_TYPE, String.class) //
        );
    }

    private static List<FieldMetadata> fieldMetadataPrep(FormDomOwnershipTableConfig config) {
        return Arrays.asList(new FieldMetadata(DataCloudConstants.AMS_ATTR_DOMAIN, String.class), //
                new FieldMetadata(ROOT_DUNS, String.class), //
                new FieldMetadata(DUNS_TYPE, String.class), //
                new FieldMetadata(TREE_NUMBER, Integer.class) //
        );
    }

}