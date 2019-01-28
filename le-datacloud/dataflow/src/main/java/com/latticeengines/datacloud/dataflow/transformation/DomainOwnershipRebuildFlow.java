package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomOwnerConstructAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.ams.DomOwnerCalRootDunsFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DomainOwnershipConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(DomainOwnershipRebuildFlow.DATAFLOW_BEAN_NAME)
public class DomainOwnershipRebuildFlow extends ConfigurableFlowBase<DomainOwnershipConfig> {
    public final static String DATAFLOW_BEAN_NAME = "DomainOwnershipRebuildFlow";
    public final static String TRANSFORMER_NAME = "FormDomOwnershipTableTransformer";

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
        return DomainOwnershipConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        DomainOwnershipConfig config = getTransformerConfig(parameters);
        Node ams = addSource(parameters.getBaseTables().get(0));

        // Get all the (domain, duns) combination from AMSeed:
        // AMSeed: find every unique domain + duns
        String expr = DataCloudConstants.AMS_ATTR_DUNS + " != null";
        Node amsWithDuns = ams.filter(expr, new FieldList(DataCloudConstants.AMS_ATTR_DUNS));
        Node amsDomDuns = amsWithDuns
                .retain(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS))
                .renamePipe("amsDomDuns");

        // Construct a table of all the duns from ams with firmographic
        // attributes of root entry appended
        Node amsDunsWithRootFirmo = appendRootDunsAndFirmo(config, amsWithDuns);

        // Construct a table of distinct (domain + rootduns) pairs with
        // firmographic attributes of root entry appended
        Node domRootDunsWithFirmo = amsDomDuns //
                .join(DataCloudConstants.AMS_ATTR_DUNS, amsDunsWithRootFirmo, DataCloudConstants.AMS_ATTR_DUNS,
                        JoinType.INNER);

        Node domOwnershipTable = constructDomOwnershipTable(domRootDunsWithFirmo, config);
        return domOwnershipTable;
    }

    // Input ams table is all the rows from ams with duns populated
    // Then append root duns and firmographic attributes of root duns entry
    private Node appendRootDunsAndFirmo(DomainOwnershipConfig config, Node ams) {
        // Dedup ams by duns and retain firmo attributes: duns, du, gu,
        // salesVol, employee, numOfLoc, primaryIndustry
        String[] arr = { DataCloudConstants.AMS_ATTR_DUNS, //
                DataCloudConstants.ATTR_GU_DUNS, //
                DataCloudConstants.ATTR_DU_DUNS, //
                DataCloudConstants.ATTR_SALES_VOL_US, //
                DataCloudConstants.ATTR_EMPLOYEE_TOTAL, //
                DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS, //
                DataCloudConstants.AMS_ATTR_PRIMARY_INDUSTRY };
        List<String> fields = new ArrayList<>(Arrays.asList(arr));
        Node amsFirmo = ams //
                .groupByAndLimit(new FieldList(DataCloudConstants.AMS_ATTR_DUNS), 1) //
                .retain(new FieldList(fields));

        // Setting RootDuns and dunsType based on selection criteria
        DomOwnerCalRootDunsFunction rootDunsFunc = new DomOwnerCalRootDunsFunction(
                new Fields(DomainOwnershipConfig.ROOT_DUNS, DomainOwnershipConfig.DUNS_TYPE));
        List<FieldMetadata> fms = new ArrayList<FieldMetadata>();
        fms.add(new FieldMetadata(DomainOwnershipConfig.ROOT_DUNS, String.class));
        fms.add(new FieldMetadata(DomainOwnershipConfig.DUNS_TYPE, String.class));
        fields.add(DomainOwnershipConfig.ROOT_DUNS);
        fields.add(DomainOwnershipConfig.DUNS_TYPE);
        Node amsFirmoWithRootDuns = amsFirmo //
                .apply(rootDunsFunc,
                        new FieldList(DataCloudConstants.ATTR_GU_DUNS, DataCloudConstants.ATTR_DU_DUNS,
                                DataCloudConstants.AMS_ATTR_DUNS),
                        fms, new FieldList(fields));

        String expr = DataCloudConstants.AMS_ATTR_DUNS + ".equals("
                + DomainOwnershipConfig.ROOT_DUNS + ")";
        Node rootOnlyFirmo = amsFirmoWithRootDuns
                .filter(expr,
                        new FieldList(DataCloudConstants.AMS_ATTR_DUNS,
                                DomainOwnershipConfig.ROOT_DUNS)) //
                .rename(new FieldList(DomainOwnershipConfig.ROOT_DUNS),
                        new FieldList(DomainOwnershipConfig.TREE_ROOT_DUNS)) //
                .retain(new FieldList(DomainOwnershipConfig.TREE_ROOT_DUNS, //
                        DataCloudConstants.ATTR_SALES_VOL_US, //
                        DataCloudConstants.ATTR_EMPLOYEE_TOTAL, //
                        DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS, //
                        DataCloudConstants.AMS_ATTR_PRIMARY_INDUSTRY)) //
                .renamePipe("RootOfTrees");
        fields.remove(DataCloudConstants.ATTR_GU_DUNS);
        fields.remove(DataCloudConstants.ATTR_DU_DUNS);
        fields.add(DomainOwnershipConfig.TREE_ROOT_DUNS);

        Node amsWithRootFirmo = amsFirmoWithRootDuns //
                .retain(DataCloudConstants.AMS_ATTR_DUNS, DomainOwnershipConfig.ROOT_DUNS,
                        DomainOwnershipConfig.DUNS_TYPE) //
                .join(DomainOwnershipConfig.ROOT_DUNS, rootOnlyFirmo,
                        DomainOwnershipConfig.TREE_ROOT_DUNS, JoinType.LEFT) //
                .retain(new FieldList(fields));
        return amsWithRootFirmo;
    }

    private Node constructDomOwnershipTable(Node domDunsWithRootFirmo, DomainOwnershipConfig config) {
        DomOwnerConstructAggregator agg = new DomOwnerConstructAggregator(
                new Fields(DataCloudConstants.AMS_ATTR_DOMAIN, //
                        DomainOwnershipConfig.ROOT_DUNS, //
                        DomainOwnershipConfig.DUNS_TYPE, //
                        DomainOwnershipConfig.TREE_NUMBER, //
                        DomainOwnershipConfig.REASON_TYPE, //
                        DomainOwnershipConfig.IS_NON_PROFITABLE),
                DomainOwnershipConfig.ROOT_DUNS, //
                DomainOwnershipConfig.TREE_ROOT_DUNS, //
                DomainOwnershipConfig.DUNS_TYPE, //
                config.getMultLargeCompThreshold(), //
                config.getFranchiseThreshold());

        List<FieldMetadata> fms = prepareFinalFms(config);
        String expr = DataCloudConstants.AMS_ATTR_DOMAIN + " != null";
        Node domainOwnershipTable = domDunsWithRootFirmo //
                .filter(expr, new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN)) //
                .groupByAndAggregate(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN), agg, fms) //
                .renamePipe("DomainRowSelect");
        return domainOwnershipTable;
    }

    private List<FieldMetadata> prepareFinalFms(DomainOwnershipConfig config) {
        return Arrays.asList(new FieldMetadata( //
                DataCloudConstants.AMS_ATTR_DOMAIN, String.class), //
                new FieldMetadata(DomainOwnershipConfig.ROOT_DUNS, String.class), //
                new FieldMetadata(DomainOwnershipConfig.DUNS_TYPE, String.class), //
                new FieldMetadata(DomainOwnershipConfig.TREE_NUMBER, Integer.class), //
                new FieldMetadata(DomainOwnershipConfig.REASON_TYPE, String.class), //
                new FieldMetadata(DomainOwnershipConfig.IS_NON_PROFITABLE, String.class) //
        );
    }

}