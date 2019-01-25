package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.OrbSecSrcSelectPriDomAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DomainOwnershipConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(CleanupOrbSecSrcFlow.DATAFLOW_BEAN_NAME)
public class CleanupOrbSecSrcFlow extends ConfigurableFlowBase<DomainOwnershipConfig> {
    public final static String DATAFLOW_BEAN_NAME = "CleanupOrbSecSrcFlow";
    public final static String TRANSFORMER_NAME = "CleanupOrbSecSrcTransformer";
    private final static String ROOT_DUNS = "ROOT_DUNS";
    private final static String ORB_SEC_PRI_DOMAIN = "PrimaryDomain";
    private final static String ORB_SRC_SEC_DOMAIN = "SecondaryDomain";
    private final static String PRIMARY_ROOT_DUNS = "PRIMARY_ROOT_DUNS";
    private final static String SECONDARY_ROOT_DUNS = "SECONDARY_ROOT_DUNS";
    private final static String MISSING_ROOT_DUNS = "MISSING_ROOT_DUNS";
    private final static String REASON_TYPE = "REASON_TYPE";

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
        Node domOwnershipTable = addSource(parameters.getBaseTables().get(0));
        Node orbSecSrc = addSource(parameters.getBaseTables().get(1));
        Node alexaRank = addSource(parameters.getBaseTables().get(2));

        // Join primary domain with ownership -> primary root duns
        Node populatePrimRootDuns = orbSecSrc //
                .join(ORB_SEC_PRI_DOMAIN, domOwnershipTable, DataCloudConstants.AMS_ATTR_DOMAIN, JoinType.LEFT) //
                .rename(new FieldList(ROOT_DUNS), new FieldList(PRIMARY_ROOT_DUNS)) //
                .retain(new FieldList(ORB_SEC_PRI_DOMAIN, ORB_SRC_SEC_DOMAIN, DataCloudConstants.AMS_ATTR_DOMAIN,
                        PRIMARY_ROOT_DUNS));

        // Join secondary domain with ownership -> secondary root duns
        Node popPrimSecRootDuns = populatePrimRootDuns //
                .join(ORB_SRC_SEC_DOMAIN, domOwnershipTable, DataCloudConstants.AMS_ATTR_DOMAIN, JoinType.LEFT) //
                .rename(new FieldList(ROOT_DUNS, DataCloudConstants.AMS_ATTR_DOMAIN),
                        new FieldList(SECONDARY_ROOT_DUNS, renameField(DataCloudConstants.AMS_ATTR_DOMAIN)));

        // SEC_ROOT_DUNS = null case -> we will retain only with REASON_TYPE =
        // MISSING_ROOT_DUNS or NULL (means doesn't exist in amSeed/domain
        // ownership table). Other reason types ->
        // FRANCHISE/MULTIPLE_LARGE_COMPANY/OTHER(firmographics same and hence
        // cannot select) : we will not retain as it
        // will add up unwanted domains. We dont consider case where primary
        // root duns doesnt exist and secondary root duns for now.
        String filterRootDuns = String.format(
                "(%s == null && (%s == null || %s.equals(\"%s\"))) || (%s != null && %s != null && %s.equals(%s))",
                SECONDARY_ROOT_DUNS, REASON_TYPE, REASON_TYPE, MISSING_ROOT_DUNS, PRIMARY_ROOT_DUNS,
                SECONDARY_ROOT_DUNS, PRIMARY_ROOT_DUNS, SECONDARY_ROOT_DUNS);

        // Compare primRootDuns and secRootDuns only when domain is matched
        popPrimSecRootDuns = popPrimSecRootDuns //
                .filter(filterRootDuns, new FieldList(PRIMARY_ROOT_DUNS, SECONDARY_ROOT_DUNS, REASON_TYPE)) //
                .retain(new FieldList(orbSecSrc.getFieldNames()));

        // populate alexa rank for primary domains
        Node popAlexaRank = popPrimSecRootDuns //
                .join(ORB_SEC_PRI_DOMAIN, alexaRank, DataCloudConstants.ALEXA_ATTR_URL, JoinType.LEFT) //
                .retain(new FieldList(ORB_SEC_PRI_DOMAIN, ORB_SRC_SEC_DOMAIN, DataCloudConstants.ALEXA_ATTR_RANK));

        // group by orbSecDom to check if more than one orbPriDomain pointing to same orbSecDomain
        // and select one of them having lower alexa rank
        OrbSecSrcSelectPriDomAggregator agg = new OrbSecSrcSelectPriDomAggregator(
                new Fields(ORB_SEC_PRI_DOMAIN, ORB_SRC_SEC_DOMAIN), ORB_SEC_PRI_DOMAIN, //
                ORB_SRC_SEC_DOMAIN, DataCloudConstants.ALEXA_ATTR_RANK);

        String expr = ORB_SEC_PRI_DOMAIN + " != null";
        Node orbSecSrcCleaned = popAlexaRank //
                .filter(expr, new FieldList(ORB_SEC_PRI_DOMAIN)) //
                .groupByAndAggregate(new FieldList(ORB_SRC_SEC_DOMAIN), agg, prepareFinalFms()) //
                .renamePipe("OrbSecSrcCleaned");
        return orbSecSrcCleaned;
    }

    private List<FieldMetadata> prepareFinalFms() {
        return Arrays.asList(new FieldMetadata(ORB_SEC_PRI_DOMAIN, String.class), //
                new FieldMetadata(ORB_SRC_SEC_DOMAIN, String.class));
    }

    private static String renameField(String field) {
        return "renamed_" + field;
    }
}
