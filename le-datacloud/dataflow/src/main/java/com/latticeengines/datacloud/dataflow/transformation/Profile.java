package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;
import com.latticeengines.dataflow.runtime.cascading.propdata.AddRandomIntFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.NumericProfileBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.NumericProfileSampleBuffer;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import cascading.tuple.Fields;

@Component(Profile.BEAN_NAME)
public class Profile extends TransformationFlowBase<BasicTransformationConfiguration, ProfileParameters> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(Profile.class);

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

        // Numeric profiling
        List<String> numAttrs = new ArrayList<>();
        Map<String, List<String>> numAttrsToDecode = new HashMap<>();// encoded attr -> {decoded attrs}
        Map<String, String> decStrs = new HashMap<>(); // decode attr -> decode strategy str
        parseNumAttrs(numAttrs, numAttrsToDecode, decStrs);
        Node numProfile = profileNumAttrs(src, numAttrs, numAttrsToDecode, decStrs);

        return numProfile;
    }

    private void parseNumAttrs(List<String> numAttrs, Map<String, List<String>> numAttrsToDecode,
            Map<String, String> decStrs) {
        Set<String> encAttrs = new HashSet<>();
        for (ProfileParameters.Attribute attr : config.getNumericAttrs()) {
            if (config.getCodeBookLookup().containsKey(attr.getAttr())) {
                if (!numAttrsToDecode.containsKey(config.getCodeBookLookup().get(attr.getAttr()))) {
                    numAttrsToDecode.put(config.getCodeBookLookup().get(attr.getAttr()), new ArrayList<>());
                }
                numAttrsToDecode.get(config.getCodeBookLookup().get(attr.getAttr())).add(attr.getAttr());
                encAttrs.add(config.getCodeBookLookup().get(attr.getAttr()));
                decStrs.put(attr.getAttr(), attr.getDecodeStrategy());
            } else {
                numAttrs.add(attr.getAttr());
            }
        }
    }

    private Node profileNumAttrs(Node src, List<String> numAttrs, Map<String, List<String>> numAttrsToDecode,
            Map<String, String> decStrs) {
        List<String> retainAttrs = new ArrayList<>();
        retainAttrs.addAll(numAttrs);
        retainAttrs.addAll(numAttrsToDecode.keySet());
        Node num = src.renamePipe("_NUM_PROFILE_").retain(new FieldList(retainAttrs));
        // Add dummy group
        num = num.apply(new AddRandomIntFunction(DUMMY_GROUP, 1, SAMPLE_SIZE, config.getRandSeed()),
                new FieldList(num.getFieldNames()), new FieldMetadata(DUMMY_GROUP, Integer.class));
        // Sampling + Depivoting
        Map<String, Class<?>> clsMap = getNumAttrClsMap(num.getSchema(), numAttrsToDecode);
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(KVDepivotStrategy.KEY_ATTR, String.class));
        fms.add(new FieldMetadata(KVDepivotStrategy.valueAttr(Integer.class), Integer.class));
        fms.add(new FieldMetadata(KVDepivotStrategy.valueAttr(Long.class), Long.class));
        fms.add(new FieldMetadata(KVDepivotStrategy.valueAttr(Float.class), Float.class));
        fms.add(new FieldMetadata(KVDepivotStrategy.valueAttr(Double.class), Double.class));
        NumericProfileSampleBuffer agg = new NumericProfileSampleBuffer(
                new Fields(KVDepivotStrategy.KEY_ATTR, KVDepivotStrategy.valueAttr(Integer.class),
                        KVDepivotStrategy.valueAttr(Long.class), KVDepivotStrategy.valueAttr(Float.class),
                        KVDepivotStrategy.valueAttr(Double.class)),
                numAttrs, clsMap, numAttrsToDecode, config.getCodeBookMap());
        num = num.groupByAndBuffer(new FieldList(DUMMY_GROUP), agg, fms);
        // Profiling
        fms = getFinalMetadata();
        NumericProfileBuffer buf = new NumericProfileBuffer(
                new Fields(DataCloudConstants.PROFILE_ATTR_ATTRNAME, DataCloudConstants.PROFILE_ATTR_SRCATTR,
                        DataCloudConstants.PROFILE_ATTR_DECSTRAT, DataCloudConstants.PROFILE_ATTR_ENCATTR,
                        DataCloudConstants.PROFILE_ATTR_LOWESTBIT, DataCloudConstants.PROFILE_ATTR_NUMBITS,
                        DataCloudConstants.PROFILE_ATTR_BKTALGO),
                KVDepivotStrategy.KEY_ATTR, clsMap, decStrs, config.isNumBucketEqualSized(), config.getBucketNum(),
                config.getMinBucketSize(), false);
        num = num.groupByAndBuffer(new FieldList(KVDepivotStrategy.KEY_ATTR), new FieldList(num.getFieldNames()), buf,
                fms);
        return num;
    }

    private Map<String, Class<?>> getNumAttrClsMap(List<FieldMetadata> schema,
            Map<String, List<String>> numAttrsToDecode) {
        Map<String, Class<?>> clsMap = new HashMap<>();
        for (FieldMetadata fm : schema) {
            if (fm.getFieldName().equals(DUMMY_GROUP)) {
                continue;
            }
            if (!numAttrsToDecode.containsKey(fm.getFieldName())) {
                clsMap.put(fm.getFieldName(), fm.getJavaType());
                continue;
            }
            List<String> decAttrs = numAttrsToDecode.get(fm.getFieldName());
            BitCodeBook book = config.getCodeBookMap().get(fm.getFieldName());
            for (String decAttr : decAttrs) {
                switch (book.getDecodeStrategy()) {
                case NUMERIC_INT:
                case NUMERIC_UNSIGNED_INT:
                    clsMap.put(decAttr, Integer.class);
                    break;
                default:
                    throw new UnsupportedOperationException(String.format(
                            "Decode strategy %s is not supported in numeric profiling", book.getDecodeStrategy()));
                }
            }
        }
        return clsMap;
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
