package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ComputeRootDunsAndTypeFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainTreeCountRowSelectAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.FormDomOwnershipTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(DomainOwnershipRebuildFlow.DATAFLOW_BEAN_NAME)
public class DomainOwnershipRebuildFlow extends ConfigurableFlowBase<FormDomOwnershipTableConfig> {
    public final static String DATAFLOW_BEAN_NAME = "FormDomainOwnershipTableFlow";
    public final static String TRANSFORMER_NAME = "FormDomOwnershipTableTransformer";
    private final static String ROOT_DUNS = "ROOT_DUNS";
    private final static String DUNS_TYPE = "DUNS_TYPE";
    private final static String TREE_NUMBER = "TREE_NUMBER";
    private final static String REASON_TYPE = "REASON_TYPE";
    private final static String IS_NON_PROFITABLE = "IS_NON_PROFITABLE";
    private final static String ORB_SEC_PRI_DOMAIN = "PrimaryDomain";
    private final static String ORB_SRC_SEC_DOMAIN = "SecondaryDomain";
    private static List<String> fieldList = new ArrayList<String>();

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
        // populating the list of fields
        populateList();
        FormDomOwnershipTableConfig config = getTransformerConfig(parameters);
        Node amSeed = addSource(parameters.getBaseTables().get(0));
        Node orbSecSrc = addSource(parameters.getBaseTables().get(1));
        // filter null duns from amSeed
        String checkNullDuns = DataCloudConstants.AMS_ATTR_DUNS + " != null";
        Node amSeedFilteredNull = amSeed //
                .filter(checkNullDuns,
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
        Node rootTypePopulated = populateRootDuns(config, amSeedFilteredNull);
        // join merged list with rootTypePopulated
        Node mergDomDunsWithRootTypePop = mergedDomDuns //
                .join(DataCloudConstants.AMS_ATTR_DUNS, rootTypePopulated, DataCloudConstants.AMS_ATTR_DUNS,
                        JoinType.INNER) //
                .groupByAndLimit(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS), 1);

        Node domOwnershipTable = constructDomOwnershipTable(mergDomDunsWithRootTypePop, config);
        return domOwnershipTable;
    }

    private static Node populateRootDuns(FormDomOwnershipTableConfig config, Node amSeed) {
        // retain (duns, du, gu, salesvolume, employee, numOfLoc)
        fieldList.remove(DataCloudConstants.AMS_ATTR_DOMAIN);
        // deduped amSeed by Duns
        Node amSeedDeduped = amSeed //
                .groupByAndLimit(new FieldList(DataCloudConstants.AMS_ATTR_DUNS), 1) //
                .retain(new FieldList(fieldList));
        String rootDunsFirmo = DataCloudConstants.AMS_ATTR_DUNS + ".equals(" + ROOT_DUNS + ")";
        // Setting RootDuns and dunsType based on selection criteria
        ComputeRootDunsAndTypeFunction computeRootDuns = new ComputeRootDunsAndTypeFunction(
                new Fields(ROOT_DUNS, DUNS_TYPE),
                DataCloudConstants.ATTR_GU_DUNS,
                DataCloudConstants.ATTR_DU_DUNS, DataCloudConstants.AMS_ATTR_DUNS);

        List<FieldMetadata> fms = new ArrayList<FieldMetadata>();
        fms.add(new FieldMetadata(ROOT_DUNS, String.class));
        fms.add(new FieldMetadata(DUNS_TYPE, String.class));
        fieldList.add(ROOT_DUNS);
        fieldList.add(DUNS_TYPE);
        Node rootTypePopulated = amSeedDeduped //
                .apply(computeRootDuns,
                        new FieldList(DataCloudConstants.ATTR_GU_DUNS, DataCloudConstants.ATTR_DU_DUNS,
                                DataCloudConstants.AMS_ATTR_DUNS),
                        fms,
                        new FieldList(fieldList));
        Node rootOfTrees = rootTypePopulated
                .filter(rootDunsFirmo, new FieldList(DataCloudConstants.AMS_ATTR_DUNS, ROOT_DUNS)) //
                .retain(new FieldList(ROOT_DUNS, DataCloudConstants.ATTR_SALES_VOL_US,
                        DataCloudConstants.ATTR_EMPLOYEE_TOTAL, DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS,
                        DataCloudConstants.AMS_ATTR_PRIMARY_INDUSTRY)) //
                .renamePipe("RootOfTrees");
        fieldList.remove(DataCloudConstants.ATTR_GU_DUNS);
        fieldList.remove(DataCloudConstants.ATTR_DU_DUNS);
        Node rootFirmoAdd = rootTypePopulated //
                .retain(DataCloudConstants.AMS_ATTR_DUNS, ROOT_DUNS, DUNS_TYPE) //
                .join(ROOT_DUNS, rootOfTrees, ROOT_DUNS, JoinType.LEFT) //
                .retain(new FieldList(fieldList));
        return rootFirmoAdd;
    }

    private static List<String> populateList() {
        fieldList.add(DataCloudConstants.AMS_ATTR_DOMAIN);
        fieldList.add(DataCloudConstants.AMS_ATTR_DUNS);
        fieldList.add(DataCloudConstants.ATTR_GU_DUNS);
        fieldList.add(DataCloudConstants.ATTR_DU_DUNS);
        fieldList.add(DataCloudConstants.ATTR_SALES_VOL_US);
        fieldList.add(DataCloudConstants.ATTR_EMPLOYEE_TOTAL);
        fieldList.add(DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS);
        fieldList.add(DataCloudConstants.AMS_ATTR_PRIMARY_INDUSTRY);
        return fieldList;
    }

    private static Node constructDomOwnershipTable(Node mergDomDunsWithRootTypePop,
            FormDomOwnershipTableConfig config) {
        DomainTreeCountRowSelectAggregator agg = new DomainTreeCountRowSelectAggregator(
                new Fields(DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS, DUNS_TYPE, TREE_NUMBER, REASON_TYPE,
                        IS_NON_PROFITABLE),
                DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS, DUNS_TYPE,
                DataCloudConstants.ATTR_SALES_VOL_US, DataCloudConstants.ATTR_EMPLOYEE_TOTAL,
                DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS, DataCloudConstants.AMS_ATTR_PRIMARY_INDUSTRY,
                config.getMultLargeCompThreshold(), config.getFranchiseThreshold());

        List<FieldMetadata> fmsForDomRowSelect = fieldMetadataWithReason(config);

        String filterNullDomain = DataCloudConstants.AMS_ATTR_DOMAIN + " != null";
        String filterRowsInSingleTree = TREE_NUMBER + " != 1";
        Node domainOwnershipTable = mergDomDunsWithRootTypePop //
                .filter(filterNullDomain, new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN)) //
                .groupByAndAggregate(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN), agg, fmsForDomRowSelect) //
                .filter(filterRowsInSingleTree, new FieldList(TREE_NUMBER)) //
                .renamePipe("DomainRowSelect");
        return domainOwnershipTable;
    }

    private static List<FieldMetadata> fieldMetadataWithReason(FormDomOwnershipTableConfig config) {
        return Arrays.asList(new FieldMetadata(DataCloudConstants.AMS_ATTR_DOMAIN, String.class), //
                new FieldMetadata(ROOT_DUNS, String.class), //
                new FieldMetadata(DUNS_TYPE, String.class), //
                new FieldMetadata(TREE_NUMBER, Integer.class), //
                new FieldMetadata(REASON_TYPE, String.class), //
                new FieldMetadata(IS_NON_PROFITABLE, String.class) //
        );
    }

}