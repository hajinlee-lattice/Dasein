package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.AMAttrEnrichParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

@Component(AMAttrEnrich.BEAN_NAME)
public class AMAttrEnrich extends TransformationFlowBase<BasicTransformationConfiguration, AMAttrEnrichParameters> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(AMSeedMerge.class);

    public static final String BEAN_NAME = "AMAttrEnrich";
    public static final String TRANSFORMER_NAME = "AMAttrEnricher";

    @Override
    public Node construct(AMAttrEnrichParameters paras) {
        Node input = addSource(paras.getBaseTables().get(0));
        // Used to mark ATTR_SOURCE in postDataflowProcessing
        List<String> inputAttrs = new ArrayList<>();
        for (String attr : input.getFieldNames()) {
            if (!attr.equals(paras.getInputLatticeId())) {
                inputAttrs.add(attr);
            }
        }
        paras.setInputAttrs(inputAttrs);
        if (paras.isNotJoinAM()) {
            return input.rename(new FieldList(paras.getInputLatticeId()),
                    new FieldList(DataCloudConstants.LATTICE_ACCOUNT_ID));
        }
        Node am = addSource(paras.getBaseTables().get(1));
        // Check field conflicts
        Set<String> customerAttrs = new HashSet<>(input.getFieldNames());
        for (String amAttr : am.getFieldNames()) {
            if (customerAttrs.contains(amAttr) && !amAttr.equals(DataCloudConstants.LATTIC_ID)
                    && !amAttr.equals(DataCloudConstants.LATTICE_ACCOUNT_ID)) {
                throw new RuntimeException("Conflict field name between customer table and account master: " + amAttr);
            }
        }
        // Resolve lattice account id
        input = input.rename(new FieldList(paras.getInputLatticeId()),
                new FieldList(renameInputId(paras.getInputLatticeId())));
        input = input.retain(new FieldList(input.getFieldNames())); // wolk-around for the bug in column alignment of rename
        am = am.rename(new FieldList(paras.getAmLatticeId()), new FieldList(renameAMId(paras.getAmLatticeId())));
        am = am.retain(new FieldList(am.getFieldNames()));// wolk-around for the bug in column alignment of rename
        Node joined = input.join(renameInputId(paras.getInputLatticeId()), am, renameAMId(paras.getAmLatticeId()),
                JoinType.LEFT);
        joined = joined
                .rename(new FieldList(renameInputId(paras.getInputLatticeId())),
                        new FieldList(DataCloudConstants.LATTICE_ACCOUNT_ID))
                .discard(renameAMId(paras.getAmLatticeId()));
        return joined;
    }

    private String renameInputId(String id) {
        return "_INPUT_" + id;
    }

    private String renameAMId(String id) {
        return "_AM_" + id;
    }

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

}
