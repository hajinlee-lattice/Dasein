package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.NumericProfileBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.ProfileSampleAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

@Component(Profile.BEAN_NAME)
public class Profile extends ConfigurableFlowBase<ProfileConfig> {

    private static final Log log = LogFactory.getLog(Profile.class);

    public static final String BEAN_NAME = "SourceProfile";
    public static final String TRANSFORMER_NAME = "SourceProfileTransformer";

    private static final String DUMMY_GROUP = "_Dummy_Group_";
    private static final String DUMMY_ROWID = "_Dummy_RowID_";
    private static final int SAMPLE_SIZE = 100000;
    private static final int COLUMN_GROUP_SIZE = 50;

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
                // if (mt.getJavaType().equals(Long.class)) {
                log.info(String.format("Numeric attr %s: %s", mt.getFieldName(), mt.getJavaType().getName()));
                numAttrs.add(mt.getFieldName());
            } else if (mt.getJavaType().equals(Boolean.class)) {
                log.info(String.format("Boolean attr %s: %s", mt.getFieldName(), mt.getJavaType().getName()));
                boolAttrs.add(mt.getFieldName());
            } else {
                log.info(String.format("Other attr %s: %s", mt.getFieldName(), mt.getJavaType().getName()));
                otherAttrs.add(mt.getFieldName());
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Node profileNumAttrs(Node source) {
        source = source.retain(new FieldList(numAttrs)).addRowID(DUMMY_ROWID).apply(
                String.format("%s %% %d", DUMMY_ROWID, SAMPLE_SIZE), new FieldList(DUMMY_ROWID),
                new FieldMetadata(DUMMY_ROWID, Long.class));
        Map<String, Class<Comparable>> cls = new HashMap<>();
        List<FieldMetadata> fms = new ArrayList<>();
        for (FieldMetadata fm : source.getSchema()) {
            if (!fm.getFieldName().equals(DUMMY_ROWID)) {
                fms.add(fm);
                cls.put(fm.getFieldName(), (Class<Comparable>) fm.getJavaType());
            }
        }
        Aggregator agg = new ProfileSampleAggregator(new Fields(numAttrs.toArray(new String[numAttrs.size()])),
                numAttrs);
        source = source.groupByAndAggregate(new FieldList(DUMMY_ROWID), agg, fms);
        List<List<String>> attrGroups = groupNumAttrs();
        Node toReturn = null;
        List<Node> toMerge = new ArrayList<>();
        for (List<String> group : attrGroups) {
            Node num = source.addColumnWithFixedValue(DUMMY_GROUP, UUID.randomUUID().toString(), String.class)
                    .renamePipe("_NODE_" + group.get(0));
            fms = new ArrayList<>();
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_ATTRNAME, String.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_SRCATTR, String.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_DECSTRAT, String.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_ENCATTR, String.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_LOWESTBIT, Integer.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_NUMBITS, Integer.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_BKTALGO, String.class));
            NumericProfileBuffer buf = new NumericProfileBuffer(
                    new Fields(DataCloudConstants.PROFILE_ATTR_ATTRNAME, DataCloudConstants.PROFILE_ATTR_SRCATTR,
                            DataCloudConstants.PROFILE_ATTR_DECSTRAT, DataCloudConstants.PROFILE_ATTR_ENCATTR,
                            DataCloudConstants.PROFILE_ATTR_LOWESTBIT, DataCloudConstants.PROFILE_ATTR_NUMBITS,
                            DataCloudConstants.PROFILE_ATTR_BKTALGO),
                    config.isNumBucketEqualSized(), config.getBucketNum(), config.getMinBucketSize(), group, cls);
            num = num.groupByAndBuffer(new FieldList(DUMMY_GROUP), buf, fms);
            if (toReturn == null) {
                toReturn = num;
            } else {
                toMerge.add(num);
            }
        }
        return toReturn.merge(toMerge);
        /*
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
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_ATTRNAME, String.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_SRCATTR, String.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_DECSTRAT, String.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_ENCATTR, String.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_LOWESTBIT, Integer.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_NUMBITS, Integer.class));
            fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_BKTALGO, String.class));
            NumericProfileBuffer buf = new NumericProfileBuffer(
                    new Fields(DataCloudConstants.PROFILE_ATTR_ATTRNAME, DataCloudConstants.PROFILE_ATTR_SRCATTR,
                            DataCloudConstants.PROFILE_ATTR_DECSTRAT, DataCloudConstants.PROFILE_ATTR_ENCATTR,
                            DataCloudConstants.PROFILE_ATTR_LOWESTBIT, DataCloudConstants.PROFILE_ATTR_NUMBITS,
                            DataCloudConstants.PROFILE_ATTR_BKTALGO),
                    numAttr,
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
        */
    }

    private List<List<String>> groupNumAttrs() {
        List<List<String>> groups = new ArrayList<>();
        for (int i = 0; i < numAttrs.size(); i++) {
            int seq = i % COLUMN_GROUP_SIZE;
            if (groups.size() < seq + 1) {
                groups.add(new ArrayList<String>());
            }
            groups.get(seq).add(numAttrs.get(i));
        }
        return groups;
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
