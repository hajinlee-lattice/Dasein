package com.latticeengines.datacloud.dataflow.transformation;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMAttrEnrichConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(AMAttrEnrich.BEAN_NAME)
public class AMAttrEnrich extends ConfigurableFlowBase<AMAttrEnrichConfig> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AMSeedMerge.class);

    public static final String BEAN_NAME = "AMAttrEnrich";

    private AMAttrEnrichConfig config;

    @Override
    public Node construct(TransformationFlowParameters paras) {
        config = getTransformerConfig(paras);
        Node input = addSource(paras.getBaseTables().get(0));
        if (config.isNotJoinAM()) {
            return input.rename(new FieldList(config.getInputLatticeId()),
                    new FieldList(DataCloudConstants.LATTICE_ACCOUNT_ID));
        }
        Node am = addSource(paras.getBaseTables().get(1));
        // Check field conflicts
        Set<String> customerAttrs = new HashSet<>(input.getFieldNames());
        for (String amAttr : am.getFieldNames()) {
            if (customerAttrs.contains(amAttr) && !amAttr.equals(DataCloudConstants.LATTICE_ID)
                    && !amAttr.equals(DataCloudConstants.LATTICE_ACCOUNT_ID)) {
                throw new RuntimeException("Conflict field name between customer table and account master: " + amAttr);
            }
        }
        // Resolve lattice account id
        input = input.rename(new FieldList(config.getInputLatticeId()),
                new FieldList(renameInputId(config.getInputLatticeId())));
        input = input.retain(new FieldList(input.getFieldNames())); // walk-around for the bug in column alignment of rename
        am = am.rename(new FieldList(config.getAmLatticeId()), new FieldList(renameAMId(config.getAmLatticeId())));
        am = am.retain(new FieldList(am.getFieldNames()));// walk-around for the bug in column alignment of rename
        Node joined = input.join(renameInputId(config.getInputLatticeId()), am, renameAMId(config.getAmLatticeId()),
                JoinType.LEFT);
        joined = joined
                .rename(new FieldList(renameInputId(config.getInputLatticeId())),
                        new FieldList(DataCloudConstants.LATTICE_ACCOUNT_ID))
                .discard(renameAMId(config.getAmLatticeId()));
        return joined;
    }

    private String renameInputId(String id) {
        return "_INPUT_" + id;
    }

    private String renameAMId(String id) {
        return "_AM_" + id;
    }

    @Override
    public String getDataFlowBeanName() {
        return BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.TRANSFORMER_AM_ENRICHER;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return AMAttrEnrichConfig.class;
    }


}
