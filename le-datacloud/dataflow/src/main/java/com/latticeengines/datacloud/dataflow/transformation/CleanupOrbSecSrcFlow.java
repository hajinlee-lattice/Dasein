package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.FormDomOwnershipTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(CleanupOrbSecSrcFlow.DATAFLOW_BEAN_NAME)
public class CleanupOrbSecSrcFlow extends ConfigurableFlowBase<FormDomOwnershipTableConfig> {
    public final static String DATAFLOW_BEAN_NAME = "CleanupOrbSecSrcFlow";
    public final static String TRANSFORMER_NAME = "CleanupOrbSecSrcTransformer";
    private final static String ROOT_DUNS = "ROOT_DUNS";
    private final static String ORB_SEC_PRI_DOMAIN = "PrimaryDomain";
    private final static String ORB_SRC_SEC_DOMAIN = "SecondaryDomain";
    private final static String PRIMARY_ROOT_DUNS = "PRIMARY_ROOT_DUNS";
    private final static String SECONDARY_ROOT_DUNS = "SECONDARY_ROOT_DUNS";
    private final static String RETAIN = "RETAIN";
    private final static String RETAIN_YES = "true";

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

        String filterRootDuns = "(" + renameField(DataCloudConstants.AMS_ATTR_DOMAIN) + " != null ) && " + "(("
                + PRIMARY_ROOT_DUNS + " != null && " + PRIMARY_ROOT_DUNS + ".equals(" + SECONDARY_ROOT_DUNS + ")))";
        String filterRetainExp = RETAIN + ".equals(\"" + RETAIN_YES + "\")";
        FieldMetadata fms = new FieldMetadata(RETAIN, String.class);
        // Compare primRootDuns and secRootDuns only when domain is matched
        popPrimSecRootDuns = popPrimSecRootDuns //
                .apply(filterRootDuns, new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN), PRIMARY_ROOT_DUNS,
                        SECONDARY_ROOT_DUNS), fms) //
                .filter(filterRetainExp, new FieldList(RETAIN)) //
                .retain(new FieldList(orbSecSrc.getFieldNames()));
        return popPrimSecRootDuns;
    }

    private static String renameField(String field) {
        return "renamed_" + field;
    }
}
