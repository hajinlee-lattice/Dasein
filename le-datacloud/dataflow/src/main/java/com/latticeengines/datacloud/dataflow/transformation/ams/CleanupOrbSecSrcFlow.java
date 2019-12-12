package com.latticeengines.datacloud.dataflow.transformation.ams;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ams.OrbSecSrcSelectPriDomAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.ams.DomainOwnershipConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(CleanupOrbSecSrcFlow.DATAFLOW_BEAN_NAME)
public class CleanupOrbSecSrcFlow extends ConfigurableFlowBase<DomainOwnershipConfig> {

    public static final String DATAFLOW_BEAN_NAME = "CleanupOrbSecSrcFlow";
    public static final String TRANSFORMER_NAME = "CleanupOrbSecSrcTransformer";

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
                .join(DomainOwnershipConfig.ORB_SEC_PRI_DOMAIN, domOwnershipTable,
                        DataCloudConstants.AMS_ATTR_DOMAIN, JoinType.LEFT) //
                .rename(new FieldList(DomainOwnershipConfig.ROOT_DUNS),
                        new FieldList(DomainOwnershipConfig.PRIMARY_ROOT_DUNS)) //
                .retain(new FieldList(DomainOwnershipConfig.ORB_SEC_PRI_DOMAIN,
                        DomainOwnershipConfig.ORB_SRC_SEC_DOMAIN,
                        DataCloudConstants.AMS_ATTR_DOMAIN,
                        DomainOwnershipConfig.PRIMARY_ROOT_DUNS));

        // Join secondary domain with ownership -> secondary root duns
        Node popPrimSecRootDuns = populatePrimRootDuns //
                .join(DomainOwnershipConfig.ORB_SRC_SEC_DOMAIN, domOwnershipTable,
                        DataCloudConstants.AMS_ATTR_DOMAIN, JoinType.LEFT) //
                .rename(new FieldList(DomainOwnershipConfig.ROOT_DUNS,
                        DataCloudConstants.AMS_ATTR_DOMAIN),
                        new FieldList(DomainOwnershipConfig.SECONDARY_ROOT_DUNS,
                                renameField(DataCloudConstants.AMS_ATTR_DOMAIN)));

        // <Expression : (SECONDARY_ROOT_DUNS == null && (REASON_TYPE == null ||
        // REASON_TYPE.equals("MISSING_ROOT_DUNS"))) || (PRIMARY_ROOT_DUNS !=
        // null && PRIMARY_ROOT_DUNS.equals(SECONDARY_ROOT_DUNS))>
        // SEC_ROOT_DUNS = null case, here we will retain only with REASON_TYPE =
        // MISSING_ROOT_DUNS or NULL (NULL here means doesn't exist in
        // amSeed/domain ownership table). Other reason types ->
        // FRANCHISE/MULTIPLE_LARGE_COMPANY/OTHER(OTHER case : firmographics same and hence
        // cannot select) : we will not retain as it
        // will add up unwanted domains. We dont consider case where primary
        // root duns doesnt exist(is null) and secondary root duns exists(not
        // null) for now.
        String filterRootDuns = String.format(
                "(%s == null && (%s == null || %s.equals(\"%s\"))) || (%s != null && %s.equals(%s))",
                DomainOwnershipConfig.SECONDARY_ROOT_DUNS, DomainOwnershipConfig.REASON_TYPE,
                DomainOwnershipConfig.REASON_TYPE, DomainOwnershipConfig.MISSING_ROOT_DUNS,
                DomainOwnershipConfig.PRIMARY_ROOT_DUNS, DomainOwnershipConfig.PRIMARY_ROOT_DUNS,
                DomainOwnershipConfig.SECONDARY_ROOT_DUNS);

        // Compare primRootDuns and secRootDuns only when domain is matched
        popPrimSecRootDuns = popPrimSecRootDuns //
                .filter(filterRootDuns,
                        new FieldList(DomainOwnershipConfig.PRIMARY_ROOT_DUNS,
                                DomainOwnershipConfig.SECONDARY_ROOT_DUNS,
                                DomainOwnershipConfig.REASON_TYPE)) //
                .retain(new FieldList(orbSecSrc.getFieldNames()));

        // populate alexa rank for primary domains
        Node popAlexaRank = popPrimSecRootDuns //
                .join(DomainOwnershipConfig.ORB_SEC_PRI_DOMAIN, alexaRank,
                        DataCloudConstants.ALEXA_ATTR_URL, JoinType.LEFT) //
                .retain(new FieldList(DomainOwnershipConfig.ORB_SEC_PRI_DOMAIN,
                        DomainOwnershipConfig.ORB_SRC_SEC_DOMAIN,
                        DataCloudConstants.ALEXA_ATTR_RANK));

        // group by orbSecDom to check if more than one orbPriDomain pointing to same orbSecDomain
        // and select one of them having lower alexa rank
        OrbSecSrcSelectPriDomAggregator agg = new OrbSecSrcSelectPriDomAggregator(
                new Fields(DomainOwnershipConfig.ORB_SEC_PRI_DOMAIN,
                        DomainOwnershipConfig.ORB_SRC_SEC_DOMAIN),
                DataCloudConstants.ALEXA_ATTR_RANK);

        String expr = DomainOwnershipConfig.ORB_SEC_PRI_DOMAIN + " != null";
        Node orbSecSrcCleaned = popAlexaRank //
                .filter(expr, new FieldList(DomainOwnershipConfig.ORB_SEC_PRI_DOMAIN)) //
                .groupByAndAggregate(new FieldList(DomainOwnershipConfig.ORB_SRC_SEC_DOMAIN), agg,
                        prepareFinalFms()) //
                .renamePipe("OrbSecSrcCleaned");
        return orbSecSrcCleaned;
    }

    private List<FieldMetadata> prepareFinalFms() {
        return Arrays.asList(
                new FieldMetadata(DomainOwnershipConfig.ORB_SEC_PRI_DOMAIN, String.class), //
                new FieldMetadata(DomainOwnershipConfig.ORB_SRC_SEC_DOMAIN, String.class));
    }

    private static String renameField(String field) {
        return "renamed_" + field;
    }
}
