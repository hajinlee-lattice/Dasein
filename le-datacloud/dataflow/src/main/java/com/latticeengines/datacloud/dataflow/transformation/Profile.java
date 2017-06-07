package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.NumericProfileBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.ProfileSampleFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(Profile.BEAN_NAME)
public class Profile extends ConfigurableFlowBase<ProfileConfig> {

    private static final Log log = LogFactory.getLog(Profile.class);

    public static final String BEAN_NAME = "SourceProfile";
    public static final String TRANSFORMER_NAME = "SourceProfileTransformer";

    private static final String DUMMY_GROUP = "_Dummy_Group_";

    private ProfileConfig config;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    private List<String> numAttrs;
    private List<String> boolAttrs;
    private List<String> otherAttrs;

    @Override
    public Node construct(TransformationFlowParameters para) {
        config = getTransformerConfig(para);
        Node src = addSource(para.getBaseTables().get(0));
        classifyAttrs(src);
        Node numProfile = profileNumAttrs(src);
        return numProfile;
    }

    private void classifyAttrs(Node src) {
        numAttrs = new ArrayList<>();
        boolAttrs = new ArrayList<>();
        otherAttrs = new ArrayList<>();
        log.info("Classifying attributes...");
        for (FieldMetadata mt : src.getSchema()) {
            if (Number.class.isAssignableFrom(mt.getJavaType())) {
                log.info(String.format("Numeric attr %s: %s", mt.getFieldName(), mt.getJavaType().getName()));
                numAttrs.add(mt.getFieldName());
            } else if (mt.getClass().equals(Boolean.class)) {
                log.info(String.format("Boolean attr %s: %s", mt.getFieldName(), mt.getJavaType().getName()));
                boolAttrs.add(mt.getFieldName());
            } else {
                log.info(String.format("Other attr %s: %s", mt.getFieldName(), mt.getJavaType().getName()));
                otherAttrs.add(mt.getFieldName());
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Node profileNumAttrs(Node source) {
        Node toReturn = null;
        List<Node> toMerge = new ArrayList<>();
        for (String numAttr : numAttrs) {
            Node num = source.retain(new FieldList(numAttr))
                    .renamePipe("_NODE_" + numAttr);
            if (config.getSampleRate() <= 0 || config.getSampleRate() > 1.0) {
                throw new IllegalArgumentException(
                        String.format("Sampling rate %f is not in range (0, 1]", config.getSampleRate()));
            }
            num = num.apply(new ProfileSampleFunction(numAttr, config.getSampleRate()), new FieldList(numAttr),
                    num.getSchema(numAttr));
            // add dummy group to put all in one group
            num = num.addColumnWithFixedValue(DUMMY_GROUP, UUID.randomUUID().toString(), String.class);
            List<FieldMetadata> fms = new ArrayList<>();
            fms.add(new FieldMetadata("_ATTR_", String.class));
            fms.add(new FieldMetadata("_NUMERIC_INTERVAL_", String.class));
            NumericProfileBuffer buf = new NumericProfileBuffer(new Fields("_ATTR_", "_NUMERIC_INTERVAL_"), numAttr,
                    config.isNumBucketEqualSized(), config.getBucketNum(), config.getMinBucketSize(),
                    (Class<Comparable>) num.getSchema(numAttr).getJavaType(), false);
            num = num.groupByAndBuffer(new FieldList(DUMMY_GROUP), buf, fms);
            if (toReturn == null) {
                toReturn = num;
            } else {
                toMerge.add(num);
            }
        }
        return toReturn.merge(toMerge);
    }

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ProfileConfig.class;
    }

}
