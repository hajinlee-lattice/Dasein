package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;
import com.latticeengines.dataflow.runtime.cascading.propdata.AddRandomIntFunction;
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

    private static final String DUMMY_GROUP = "_Dummy_Group_";
    private static final int SAMPLE_SIZE = 100000;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    private ProfileParameters config;

    @Override
    public Node construct(ProfileParameters para) {
        this.config = para;
        Node src = addSource(para.getBaseTables().get(0));
        List<String> numAttrs = parseNumAttrs();
        Node numProfile = profileNumAttrs(src, numAttrs);
        return numProfile;
    }

    private List<String> parseNumAttrs() {
        List<String> numAttrs = new ArrayList<>();
        for (ProfileParameters.Attribute attr : config.getNumericAttrs()) {
            numAttrs.add(attr.getAttr());
        }
        return numAttrs;
    }

    @SuppressWarnings("rawtypes")
    private Node profileNumAttrs(Node src, List<String> numAttrs) {
        Node num = src.renamePipe("_NUM_PROFILE_").retain(new FieldList(numAttrs));
        num = num.apply(new AddRandomIntFunction(DUMMY_GROUP, 1, SAMPLE_SIZE, config.getRandSeed()),
                new FieldList(num.getFieldNames()), new FieldMetadata(DUMMY_GROUP, Integer.class));
        Map<String, Class<?>> cls = new HashMap<>();
        List<FieldMetadata> fms = new ArrayList<>();
        for (FieldMetadata fm : num.getSchema()) {
            if (!fm.getFieldName().equals(DUMMY_GROUP)) {
                fms.add(fm);
                cls.put(fm.getFieldName(), fm.getJavaType());
            }
        }
        // Sampling
        Aggregator agg = new ProfileSampleAggregator(new Fields(numAttrs.toArray(new String[numAttrs.size()])),
                numAttrs);
        num = num.groupByAndAggregate(new FieldList(DUMMY_GROUP), agg, fms);
        num = num.kvDepivot(new FieldList(), new FieldList(DUMMY_GROUP));
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
