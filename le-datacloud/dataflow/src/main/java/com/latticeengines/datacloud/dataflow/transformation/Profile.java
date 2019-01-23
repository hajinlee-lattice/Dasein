package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;
import com.latticeengines.dataflow.runtime.cascading.propdata.AddRandomIntFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.CategoricalProfileBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.CategoricalProfileGroupingBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.DiscreteProfileBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.DiscreteProfileGroupingBuffer;
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
public class Profile
        extends TransformationFlowBase<BasicTransformationConfiguration, ProfileParameters> {

    public static final String BEAN_NAME = "SourceProfile";

    private static final String DUMMY_GROUP = "_Dummy_Group_";
    private static final int SAMPLE_SIZE = 100000;
    private static final String NONSEG_FLAG = "__LATTICE_NONCAT_FLAG_F167B732-D33b-40D0-8288-87605A82650C__";
    private static final String CAT_ATTR = "CatAttr";
    private static final String CAT_VALUE = "CatValue";

    @Inject
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    private ProfileParameters config;

    @Override
    public Node construct(ProfileParameters para) {
        this.config = para;
        Node src = addSource(para.getBaseTables().get(0));

        // Preparation
        List<String> numAttrs = new ArrayList<>();
        // encoded attr -> {decoded attrs}
        Map<String, List<String>> numAttrsToDecode = new HashMap<>();
        // decode attr -> decode strategy str
        Map<String, String> decStrs = new HashMap<>();
        parseNumAttrs(numAttrs, numAttrsToDecode, decStrs);
        List<String> catAttrs = new ArrayList<>();
        parseCatAttrs(catAttrs);

        // Add dummy group
        src = retainAttrs(src, numAttrs, numAttrsToDecode, catAttrs).checkpoint();
        src = src.apply(new AddRandomIntFunction(DUMMY_GROUP, 1, SAMPLE_SIZE, config.getRandSeed()),
                new FieldList(src.getFieldNames()), new FieldMetadata(DUMMY_GROUP, Integer.class));

        // Discrete profiling
        Node disProfile = profileDisAttrs(src, numAttrs, numAttrsToDecode, decStrs);

        // Numeric profiling
        Node numProfile = profileNumAttrs(src, numAttrs, numAttrsToDecode, decStrs);

        // Categorical profiling
        Node catProfile = profileCatAttrs(src, catAttrs);

        // Exclude discrete attrs from numeric attrs
        numProfile = cleanNumProfile(numProfile, disProfile);

        List<Node> mergeList = new ArrayList<>();
        mergeList.add(catProfile);
        mergeList.add(disProfile);
        return numProfile.merge(mergeList);
    }

    private Node retainAttrs(Node src, List<String> numAttrs,
            Map<String, List<String>> numAttrsToDecode, List<String> catAttrs) {
        List<String> retainAttrs = new ArrayList<>();
        retainAttrs.addAll(numAttrs);
        retainAttrs.addAll(numAttrsToDecode.keySet());
        retainAttrs.addAll(catAttrs);
        src = src.retain(new FieldList(retainAttrs));
        return src;
    }

    private void parseNumAttrs(List<String> numAttrs, Map<String, List<String>> numAttrsToDecode,
            Map<String, String> decStrs) {
        for (ProfileParameters.Attribute attr : config.getNumericAttrs()) {
            if (config.getCodeBookLookup().containsKey(attr.getAttr())) {
                if (!numAttrsToDecode.containsKey(config.getCodeBookLookup().get(attr.getAttr()))) {
                    numAttrsToDecode.put(config.getCodeBookLookup().get(attr.getAttr()),
                            new ArrayList<>());
                }
                numAttrsToDecode.get(config.getCodeBookLookup().get(attr.getAttr()))
                        .add(attr.getAttr());
                decStrs.put(attr.getAttr(), attr.getDecodeStrategy());
            } else {
                numAttrs.add(attr.getAttr());
            }
        }
    }

    private void parseCatAttrs(List<String> catAttrs) {
        for (ProfileParameters.Attribute attr : config.getCatAttrs()) {
            catAttrs.add(attr.getAttr());
        }
    }

    private Node profileDisAttrs(Node src, List<String> numAttrs,
            Map<String, List<String>> numAttrsToDecode, Map<String, String> decStrs) {
        List<String> retainAttrs = new ArrayList<>();
        retainAttrs.addAll(numAttrs);
        retainAttrs.addAll(numAttrsToDecode.keySet());
        retainAttrs.add(DUMMY_GROUP);
        Node dis = src.renamePipe("_DIS_PROFILE_").retain(new FieldList(retainAttrs));
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(KVDepivotStrategy.KEY_ATTR, String.class));
        fms.add(new FieldMetadata(KVDepivotStrategy.valueAttr(Integer.class), Integer.class));
        fms.add(new FieldMetadata(KVDepivotStrategy.valueAttr(Long.class), Long.class));
        DiscreteProfileGroupingBuffer groupBuf = new DiscreteProfileGroupingBuffer(
                new Fields(KVDepivotStrategy.KEY_ATTR, KVDepivotStrategy.valueAttr(Integer.class),
                        KVDepivotStrategy.valueAttr(Long.class)),
                numAttrs, numAttrsToDecode, config.getCodeBookMap(), config.getMaxDiscrete());
        dis = dis.groupByAndBuffer(new FieldList(DUMMY_GROUP), groupBuf, fms);
        fms = getFinalMetadata();
        DiscreteProfileBuffer buf = new DiscreteProfileBuffer(new Fields(
                DataCloudConstants.PROFILE_ATTR_ATTRNAME, DataCloudConstants.PROFILE_ATTR_SRCATTR,
                DataCloudConstants.PROFILE_ATTR_DECSTRAT, DataCloudConstants.PROFILE_ATTR_ENCATTR,
                DataCloudConstants.PROFILE_ATTR_LOWESTBIT, DataCloudConstants.PROFILE_ATTR_NUMBITS,
                DataCloudConstants.PROFILE_ATTR_BKTALGO), config.getMaxDiscrete(), decStrs);
        dis = dis.groupByAndBuffer(new FieldList(KVDepivotStrategy.KEY_ATTR),
                new FieldList(dis.getFieldNames()), buf, fms);
        return dis;
    }

    private Node profileNumAttrs(Node src, List<String> numAttrs,
            Map<String, List<String>> numAttrsToDecode, Map<String, String> decStrs) {
        List<String> retainAttrs = new ArrayList<>();
        retainAttrs.addAll(numAttrs);
        retainAttrs.addAll(numAttrsToDecode.keySet());
        retainAttrs.add(DUMMY_GROUP);
        Node num = src.renamePipe("_NUM_PROFILE_").retain(new FieldList(retainAttrs));
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
                        KVDepivotStrategy.valueAttr(Long.class),
                        KVDepivotStrategy.valueAttr(Float.class),
                        KVDepivotStrategy.valueAttr(Double.class)),
                numAttrs, clsMap, numAttrsToDecode, config.getCodeBookMap());
        num = num.groupByAndBuffer(new FieldList(DUMMY_GROUP), agg, fms);
        // Profiling
        fms = getFinalMetadata();
        NumericProfileBuffer buf = new NumericProfileBuffer(new Fields(
                DataCloudConstants.PROFILE_ATTR_ATTRNAME, DataCloudConstants.PROFILE_ATTR_SRCATTR,
                DataCloudConstants.PROFILE_ATTR_DECSTRAT, DataCloudConstants.PROFILE_ATTR_ENCATTR,
                DataCloudConstants.PROFILE_ATTR_LOWESTBIT, DataCloudConstants.PROFILE_ATTR_NUMBITS,
                DataCloudConstants.PROFILE_ATTR_BKTALGO), KVDepivotStrategy.KEY_ATTR, clsMap,
                decStrs, config.isNumBucketEqualSized(), config.getBucketNum(),
                config.getMinBucketSize(), false);
        num = num.groupByAndBuffer(new FieldList(KVDepivotStrategy.KEY_ATTR), buf, fms);
        return num;
    }

    private Node profileCatAttrs(Node src, List<String> catAttrs) {
        List<String> retainAttrs = new ArrayList<>();
        retainAttrs.addAll(catAttrs);
        retainAttrs.add(DUMMY_GROUP);
        Node cat = src.renamePipe("_CAT_PROFILE_").retain(new FieldList(retainAttrs));
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(CAT_ATTR, String.class));
        fms.add(new FieldMetadata(CAT_VALUE, String.class));
        CategoricalProfileGroupingBuffer groupBuf = new CategoricalProfileGroupingBuffer(
                new Fields(CAT_ATTR, CAT_VALUE), CAT_ATTR, CAT_VALUE, NONSEG_FLAG,
                config.getMaxCats(), config.getMaxCatLength(), catAttrs);
        cat = cat.groupByAndBuffer(new FieldList(DUMMY_GROUP), groupBuf, fms);
        fms = getFinalMetadata();
        CategoricalProfileBuffer buf = new CategoricalProfileBuffer(new Fields(
                DataCloudConstants.PROFILE_ATTR_ATTRNAME, DataCloudConstants.PROFILE_ATTR_SRCATTR,
                DataCloudConstants.PROFILE_ATTR_DECSTRAT, DataCloudConstants.PROFILE_ATTR_ENCATTR,
                DataCloudConstants.PROFILE_ATTR_LOWESTBIT, DataCloudConstants.PROFILE_ATTR_NUMBITS,
                DataCloudConstants.PROFILE_ATTR_BKTALGO), CAT_ATTR, CAT_VALUE, NONSEG_FLAG,
                config.getMaxCats());
        cat = cat.groupByAndBuffer(new FieldList(CAT_ATTR), new FieldList(cat.getFieldNames()), buf,
                fms);
        return cat;
    }

    private Node cleanNumProfile(Node numProfile, Node disProfile) {
        List<String> renamedDisSchema = new ArrayList<>();
        disProfile.getFieldNames().forEach(name -> renamedDisSchema.add("DIS_" + name));
        disProfile = disProfile.rename(new FieldList(disProfile.getFieldNames()),
                new FieldList(renamedDisSchema));
        List<String> retainNumSchema = numProfile.getFieldNames();
        numProfile = numProfile
                .join(new FieldList(DataCloudConstants.PROFILE_ATTR_ATTRNAME), disProfile,
                        new FieldList("DIS_" + DataCloudConstants.PROFILE_ATTR_ATTRNAME),
                        JoinType.LEFT)
                .filter("DIS_" + DataCloudConstants.PROFILE_ATTR_ATTRNAME + " == null",
                        new FieldList("DIS_" + DataCloudConstants.PROFILE_ATTR_ATTRNAME))
                .retain(new FieldList(retainNumSchema));
        return numProfile;
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
                                "Decode strategy %s is not supported in numeric profiling",
                                book.getDecodeStrategy()));
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
