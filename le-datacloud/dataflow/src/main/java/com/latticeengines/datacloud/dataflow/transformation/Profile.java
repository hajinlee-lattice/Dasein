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
import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;
import com.latticeengines.dataflow.runtime.cascading.propdata.NumericProfileBuf;
import com.latticeengines.dataflow.runtime.cascading.propdata.NumericProfileBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.ProfileSampleAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

@Component(Profile.BEAN_NAME)
public class Profile extends TransformationFlowBase<BasicTransformationConfiguration, ProfileParameters> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(Profile.class);

    public static final String BEAN_NAME = "SourceProfile";
    public static final String TRANSFORMER_NAME = "SourceProfiler";

    private static final String DUMMY_GROUP = "_Dummy_Group_";
    private static final String DUMMY_ROWID = "_Dummy_RowID_";
    private static final int SAMPLE_SIZE = 100000;
    private static final int NUM_ATTR_GROUPS = 10;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    private ProfileParameters config;

    @Override
    public Node construct(ProfileParameters para) {
        this.config = para;
        Node src = addSource(para.getBaseTables().get(0));
        Node numProfile = profileNumAttrs(src);
        return numProfile;
    }

    @SuppressWarnings("rawtypes")
    private Node profileNumAttrs(Node src) {
        Node num = src.renamePipe("_NUM_PROFILE_").retain(new FieldList(config.getNumAttrs())).addRowID(DUMMY_ROWID)
                .apply(String.format("%s %% %d", DUMMY_ROWID, SAMPLE_SIZE), new FieldList(DUMMY_ROWID),
                new FieldMetadata(DUMMY_ROWID, Long.class));
        Map<String, Class<?>> cls = new HashMap<>();
        List<FieldMetadata> fms = new ArrayList<>();
        for (FieldMetadata fm : num.getSchema()) {
            if (!fm.getFieldName().equals(DUMMY_ROWID)) {
                fms.add(fm);
                cls.put(fm.getFieldName(), fm.getJavaType());
            }
        }
        // Sampling
        Aggregator agg = new ProfileSampleAggregator(
                new Fields(config.getNumAttrs().toArray(new String[config.getNumAttrs().size()])),
                config.getNumAttrs());
        num = num.groupByAndAggregate(new FieldList(DUMMY_ROWID), agg, fms);
        num = num.kvDepivot(new FieldList(), new FieldList(DUMMY_ROWID));
        // Profiling
        fms = getFinalMetadata();
        NumericProfileBuffer buf = new NumericProfileBuffer(
                new Fields(DataCloudConstants.PROFILE_ATTR_ATTRNAME, DataCloudConstants.PROFILE_ATTR_SRCATTR,
                        DataCloudConstants.PROFILE_ATTR_DECSTRAT, DataCloudConstants.PROFILE_ATTR_ENCATTR,
                        DataCloudConstants.PROFILE_ATTR_LOWESTBIT, DataCloudConstants.PROFILE_ATTR_NUMBITS,
                        DataCloudConstants.PROFILE_ATTR_BKTALGO),
                KVDepivotStrategy.KEY_ATTR, cls, config.isNumBucketEqualSized(), config.getBucketNum(),
                config.getMinBucketSize(), false);
        num = num.groupByAndBuffer(new FieldList(KVDepivotStrategy.KEY_ATTR), new FieldList(num.getFieldNames()), buf,
                fms);
        return num;
    }

    @Deprecated
    @SuppressWarnings({ "rawtypes", "unchecked", "unused" })
    private Node profileNumAttrsGroupByColumn(Node src) {
        src = src.retain(new FieldList(config.getNumAttrs())).addRowID(DUMMY_ROWID).apply(
                String.format("%s %% %d", DUMMY_ROWID, SAMPLE_SIZE), new FieldList(DUMMY_ROWID),
                new FieldMetadata(DUMMY_ROWID, Long.class));
        Map<String, Class<Comparable>> cls = new HashMap<>();
        List<FieldMetadata> fms = new ArrayList<>();
        for (FieldMetadata fm : src.getSchema()) {
            if (!fm.getFieldName().equals(DUMMY_ROWID)) {
                fms.add(fm);
                cls.put(fm.getFieldName(), (Class<Comparable>) fm.getJavaType());
            }
        }
        Aggregator agg = new ProfileSampleAggregator(
                new Fields(config.getNumAttrs().toArray(new String[config.getNumAttrs().size()])),
                config.getNumAttrs());
        src = src.groupByAndAggregate(new FieldList(DUMMY_ROWID), agg, fms);
        List<List<String>> attrGroups = groupNumAttrs();
        Node toReturn = null;
        List<Node> toMerge = new ArrayList<>();
        for (List<String> group : attrGroups) {
            Node num = src.addColumnWithFixedValue(DUMMY_GROUP, UUID.randomUUID().toString(), String.class)
                    .renamePipe("_NUM_NODE_" + group.get(0));
            fms = getFinalMetadata();
            NumericProfileBuf buf = new NumericProfileBuf(
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
    }

    private List<List<String>> groupNumAttrs() {
        List<List<String>> groups = new ArrayList<>();
        for (int i = 0; i < config.getNumAttrs().size(); i++) {
            int seq = i % NUM_ATTR_GROUPS;
            if (groups.size() < seq + 1) {
                groups.add(new ArrayList<String>());
            }
            groups.get(seq).add(config.getNumAttrs().get(i));
        }
        return groups;
    }

    private List<FieldMetadata> getFinalMetadata() {
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_ATTRNAME, String.class));
        fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_SRCATTR, String.class));
        fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_DECSTRAT, String.class));
        fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_ENCATTR, String.class));
        fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_LOWESTBIT, Integer.class));
        fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_NUMBITS, Integer.class));
        fms.add(new FieldMetadata(DataCloudConstants.PROFILE_ATTR_BKTALGO, String.class));
        return fms;
    }

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

}
