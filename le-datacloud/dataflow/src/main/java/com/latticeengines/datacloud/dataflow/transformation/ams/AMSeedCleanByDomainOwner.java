package com.latticeengines.datacloud.dataflow.transformation.ams;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ams.AMSeedCleanByDomOwnFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ams.DomOwnerCalRootDunsFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ams.UpdatePriDomAlexaRankFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.ams.DomainOwnershipConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;

import cascading.tuple.Fields;

@Component(AMSeedCleanByDomainOwner.DATAFLOW_BEAN_NAME)
public class AMSeedCleanByDomainOwner extends ConfigurableFlowBase<DomainOwnershipConfig> {
    public static final String DATAFLOW_BEAN_NAME = DataCloudConstants.TRANSFORMER_AMS_CLEANBY_DOM_OWNER + "Flow";
    public static final String TRANSFORMER_NAME = DataCloudConstants.TRANSFORMER_AMS_CLEANBY_DOM_OWNER;

    private static final String ROOT_DUNS = DomainOwnershipConfig.ROOT_DUNS;
    private static final String DUNS_TYPE = DomainOwnershipConfig.DUNS_TYPE;
    private static final String TREE_NUMBER = DomainOwnershipConfig.TREE_NUMBER;
    private static final String REASON_TYPE = DomainOwnershipConfig.REASON_TYPE;
    private static final String AMS_DOMAIN = DataCloudConstants.AMS_ATTR_DOMAIN;
    private static final String AMS_DUNS = DataCloudConstants.AMS_ATTR_DUNS;
    private static final String AMS_GU_DUNS = DataCloudConstants.ATTR_GU_DUNS;
    private static final String AMS_DU_DUNS = DataCloudConstants.ATTR_DU_DUNS;
    private static final String AMS_DOM_SRC = DataCloudConstants.AMS_ATTR_DOMAIN_SOURCE;
    private static final String ALEXA_RANK = DataCloudConstants.ALEXA_ATTR_RANK; // AlexaMostRecent Rank field
    private static final String ORBSEC_PRIDOM = DataCloudConstants.ORBSEC_ATTR_PRIDOM;
    private static final String ORBSRC_SECDOM = DataCloudConstants.ORBSEC_ATTR_SECDOM;

    private static final String PRI_DOM_ALEXA_RANK = "PRI_DOM_ALEXA_RANK";

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
        Node domainOwner = addSource(parameters.getBaseTables().get(0));
        Node ams = addSource(parameters.getBaseTables().get(1));
        // OrbSeconaryDomain has been cleaned by domain ownership table in
        // previous pipeline step
        Node orbSecDomain = addSource(parameters.getBaseTables().get(2));
        Node alexa = addSource(parameters.getBaseTables().get(3));
        if (ams.getSchema(OperationLogUtils.DEFAULT_FIELD_NAME) == null) {
            ams = ams.addColumnWithFixedValue(OperationLogUtils.DEFAULT_FIELD_NAME, null, String.class);
        }
        List<String> amsFields = ams.getFieldNames();

        // Following parts clean up ams domains which exist in domain ownership
        // table with tree number != 1

        // Find domains existing in multiple trees
        domainOwner = domainOwner.filter(TREE_NUMBER + " != 1", new FieldList(TREE_NUMBER)); //
        // Find domain ownership that are used for ams cleanup
        // Retain columns : domain, root duns, root duns type
        domainOwner = domainOwner //
                .filter(ROOT_DUNS + " != null", new FieldList(ROOT_DUNS)) //
                .rename(new FieldList(AMS_DOMAIN, ROOT_DUNS),
                        new FieldList(renameField(AMS_DOMAIN), renameField(ROOT_DUNS))) //
                .retain(new FieldList(renameField(AMS_DOMAIN), renameField(ROOT_DUNS),
                        DUNS_TYPE, REASON_TYPE));
        Node amsClean = computeRootDunsAndCompare(ams, domainOwner) //
                .groupByAndLimit(new FieldList(AMS_DOMAIN, AMS_DUNS), 1);

        // Following parts handles the case that ams domains exist in orbsec as
        // secondary domain, then replace the domain with orb primary domain and
        // update corresponding alexa rank

        // Add alexa rank of primary domain to orb secondary domain
        Node orbWithAlexaRank = orbSecDomain //
                .join(ORBSEC_PRIDOM, alexa, DataCloudConstants.ATTR_ALEXA_DOMAIN, JoinType.LEFT) //
                .rename(new FieldList(ALEXA_RANK), new FieldList(PRI_DOM_ALEXA_RANK)) //
                .retain(new FieldList(ORBSEC_PRIDOM, ORBSRC_SECDOM, PRI_DOM_ALEXA_RANK))
                // Currently there is no duplicate secondary domain in OrbSec
                // source. Add a safeguard in case duplicates are introduced
                .groupByAndLimit(new FieldList(ORBSRC_SECDOM), 1);
        // Left join ams with orb secondary domain by (domain, secDomain)
        Node amsJoinOrb = amsClean //
                .join(AMS_DOMAIN, orbWithAlexaRank, ORBSRC_SECDOM, JoinType.LEFT);
        // Apply function for replace domain with priDom and update alexa rank
        UpdatePriDomAlexaRankFunction updateFunction = new UpdatePriDomAlexaRankFunction(
                new Fields(amsJoinOrb.getFieldNamesArray()), PRI_DOM_ALEXA_RANK);
        amsJoinOrb = amsJoinOrb //
                .apply(updateFunction, new FieldList(amsJoinOrb.getFieldNames()), amsJoinOrb.getSchema(),
                        new FieldList(amsJoinOrb.getFieldNames()), Fields.REPLACE) //
                .retain(new FieldList(amsFields)) //
                // Reason to dedup here:
                // If ams domain is secdom in Orb, UpdatePriDomAlexaRankFunction
                // will update the domain to orb pridom.
                // But for same duns, ams might already have another domain same
                // as orb pridom, so we will have duplicate.
                // Sort by domain source does not have business purpose, just to
                // have a deterministic dedup behavior
                .groupByAndLimit(new FieldList(AMS_DOMAIN, AMS_DUNS), new FieldList(AMS_DOM_SRC), 1, false, false);
        return amsJoinOrb;
    }

    private Node computeRootDunsAndCompare(Node ams, Node domainOwner) {
        // add ROOT_DUNS & DUNS_TYPE to ams
        DomOwnerCalRootDunsFunction computeRootDuns = new DomOwnerCalRootDunsFunction(new Fields(ROOT_DUNS, DUNS_TYPE));
        List<FieldMetadata> fms = new ArrayList<FieldMetadata>();
        fms.add(new FieldMetadata(ROOT_DUNS, String.class));
        fms.add(new FieldMetadata(DUNS_TYPE, String.class));
        List<String> fields = new ArrayList<>(ams.getFieldNames());
        fields.add(ROOT_DUNS);
        fields.add(DUNS_TYPE);
        Node amsWithRootDuns = ams.apply(computeRootDuns, new FieldList(AMS_GU_DUNS, AMS_DU_DUNS, AMS_DUNS), fms,
                new FieldList(fields));

        // ams left join domain owner
        Node joinAMSWithDomOwn = amsWithRootDuns //
                .join(new FieldList(AMS_DOMAIN), domainOwner, new FieldList(renameField(AMS_DOMAIN)), JoinType.LEFT);

        // function to check if ROOT_DUNS of ams and domain owner
        // are same : if equal then retain and if not equal then don't retain
        // (mark domain = null)
        AMSeedCleanByDomOwnFunction func = new AMSeedCleanByDomOwnFunction(
                new Fields(joinAMSWithDomOwn.getFieldNamesArray()), renameField(AMS_DOMAIN), renameField(ROOT_DUNS));
        Node amsClean = joinAMSWithDomOwn
                .apply(func,
                        new FieldList(joinAMSWithDomOwn.getFieldNames()), joinAMSWithDomOwn.getSchema(),
                        new FieldList(joinAMSWithDomOwn.getFieldNames()), Fields.REPLACE) //
                .retain(new FieldList(ams.getFieldNames()));
        return amsClean;
    }

    private String renameField(String field) {
        return "_Renamed_" + field;
    }

}
