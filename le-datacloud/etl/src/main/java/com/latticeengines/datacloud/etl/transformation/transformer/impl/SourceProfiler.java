package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceProfiler.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.metadata.FundamentalType.AVRO_PROP_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.BitCodeBookUtils;
import com.latticeengines.datacloud.dataflow.transformation.Profile;
import com.latticeengines.datacloud.etl.transformation.TransformerUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategorizedIntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.ProfileParameters.Attribute;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.FundamentalType;


@Component(TRANSFORMER_NAME)
public class SourceProfiler extends AbstractDataflowTransformer<ProfileConfig, ProfileParameters> {
    private static final Logger log = LoggerFactory.getLogger(SourceProfiler.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_PROFILER;
    public static final String AM_PROFILE = "AMProfile";
    public static final String IS_PROFILE = "IsProfile";
    public static final String NO_BUCKET = "NoBucket";
    public static final String DECODE_STRATEGY = "DecodeStrategy";
    public static final String ENCODED_COLUMN = "EncodedColumn";
    public static final String NUM_BITS = "NumBits";
    public static final String BKT_ALGO = "BktAlgo";
    public static final String VALUE_DICT = "ValueDict";

    @Value("${datacloud.etl.profile.encode.bit:64}")
    private int encodeBits;

    @Value("${datacloud.etl.profile.attrs:1000}")
    private int maxAttrs;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private SourceAttributeEntityMgr srcAttrEntityMgr;

    @Autowired
    private DataCloudVersionService dataCloudVersionService;

    private static final Set<Schema.Type> NUM_TYPES = new HashSet<>(Arrays
            .asList(new Schema.Type[] { Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE }));
    private static final Set<Schema.Type> BOOL_TYPES = new HashSet<>(
            Arrays.asList(new Schema.Type[] { Schema.Type.BOOLEAN }));
    private static final Set<Schema.Type> CAT_TYPES = new HashSet<>(
            Arrays.asList(new Schema.Type[] { Schema.Type.STRING }));

    @Override
    protected String getDataFlowBeanName() {
        return Profile.BEAN_NAME;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return ProfileConfig.class;
    }

    @Override
    protected Class<ProfileParameters> getDataFlowParametersClass() {
        return ProfileParameters.class;
    }

    @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir, ProfileParameters paras,
            ProfileConfig config) {
        initProfileParameters(config, paras);
        classifyAttrs(step.getBaseSources()[0], step.getBaseVersions().get(0), config, paras);
    }

    @Override
    protected void postDataFlowProcessing(TransformStep step, String workflowDir, ProfileParameters paras,
            ProfileConfig config) {
        if (config.getStage().equals(DataCloudConstants.PROFILE_STAGE_ENRICH)) {
            postProcessProfiledAttrs(workflowDir, paras);
        }
        List<Object[]> result = new ArrayList<>();
        if (paras.getIdAttr() != null) {
            result.add(profileIdAttr(paras.getIdAttr()));
        }
        for (ProfileParameters.Attribute attr : paras.getAttrsToRetain()) {
            result.add(profileAttrToRetain(attr));
        }
        Map<String, List<ProfileParameters.Attribute>> amAttrsToEnc = groupAttrsToEnc(paras.getAmAttrsToEnc(),
                DataCloudConstants.EAttr);
        Map<String, List<ProfileParameters.Attribute>> exAttrsToEnc = groupAttrsToEnc(paras.getExAttrsToEnc(),
                StringUtils.isBlank(config.getEncAttrPrefix()) ? DataCloudConstants.CEAttr : config.getEncAttrPrefix());

        reviewFinalAttrGroup(result, amAttrsToEnc, exAttrsToEnc, config, paras);
        for (Map.Entry<String, List<ProfileParameters.Attribute>> entry : amAttrsToEnc.entrySet()) {
            int lowestBit = 0;
            for (ProfileParameters.Attribute attr : entry.getValue()) {
                result.add(profileAttrToEnc(attr, entry.getKey(), lowestBit));
                lowestBit += attr.getEncodeBitUnit();
            }
        }
        for (Map.Entry<String, List<ProfileParameters.Attribute>> entry : exAttrsToEnc.entrySet()) {
            int lowestBit = 0;
            for (ProfileParameters.Attribute attr : entry.getValue()) {
                result.add(profileAttrToEnc(attr, entry.getKey(), lowestBit));
                lowestBit += attr.getEncodeBitUnit();
            }
        }
        Object[][] data = new Object[result.size()][7];
        for (int i = 0; i < result.size(); i++) {
            data[i] = result.get(i);
        }
        List<Pair<String, Class<?>>> columns = prepareColumns();
        uploadAvro(workflowDir, columns, data);
    }

    @Override
    protected void updateStepCount(TransformStep step, String workflowDir) {
        try {
            Long targetRecords = AvroUtils.count(yarnConfiguration, workflowDir + "/*.avro");
            step.setCount(targetRecords);
        } catch (Exception ex) {
            log.error(String.format("Fail to count records in %s", workflowDir), ex);
        }
    }

    private void initProfileParameters(ProfileConfig config, ProfileParameters paras) {
        paras.setNumericAttrs(new ArrayList<>());
        paras.setCatAttrs(new ArrayList<>());
        paras.setAttrsToRetain(new ArrayList<>());
        paras.setAmAttrsToEnc(new ArrayList<>());
        paras.setExAttrsToEnc(new ArrayList<>());
        paras.setCodeBookMap(new HashMap<>());
        paras.setCodeBookLookup(new HashMap<>());
        paras.setNumBucketEqualSized(config.isNumBucketEqualSized());
        paras.setBucketNum(config.getBucketNum());
        paras.setMinBucketSize(config.getMinBucketSize());
        paras.setRandSeed(config.getRandSeed());
        paras.setEncAttrPrefix(config.getEncAttrPrefix());
        paras.setMaxCats(config.getMaxCat());
        paras.setMaxCatLength(config.getMaxCatLength());
        paras.setCatAttrsNotEnc(config.getCatAttrsNotEnc());
        paras.setMaxDiscrete(config.getMaxDiscrete());
    }

    private void classifyAttrs(Source baseSrc, String baseVer, ProfileConfig config, ProfileParameters paras) {
        String dataCloudVersion = findDCVersionToProfile(config);
        Map<String, ProfileArgument> amAttrsConfig = findAMAttrsConfig(config, dataCloudVersion);
        Schema schema = findSchema(baseSrc, baseVer);

        log.info("Classifying attributes...");
        try {
            // Parse configuration for attrs already encoded in the profiled source which need to decode
            Map<String, List<ProfileParameters.Attribute>> encAttrsMap = new HashMap<>(); // encoded attr -> [decoded attrs] (Enabled in profiling)
            Set<String> encAttrs = new HashSet<>(); // all encoded attrs (enabled/disabled in profiling)
            Map<String, ProfileParameters.Attribute> decAttrsMap = new HashMap<>();  // decoded attr name -> decoded attr object
            Map<String, String> decodeStrs = new HashMap<>();   // decoded attr -> decode strategy str
            parseEncodedAttrsConfig(amAttrsConfig, encAttrsMap, encAttrs, decAttrsMap, decodeStrs);
            // Build BitCodeBook
            BitCodeBookUtils.constructCodeBookMap(paras.getCodeBookMap(), paras.getCodeBookLookup(), decodeStrs);
            // Parse flat attrs in the profiled source
            for (Field field : schema.getFields()) {
                boolean readyForNext = false;
                readyForNext = AttrClassifier.isIdAttr(field, paras);
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isAttrToDiscard(field, amAttrsConfig);
                }
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isAttrNoBucket(field, amAttrsConfig, paras);
                }
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isPreknownAttr(field, encAttrsMap, decAttrsMap, encAttrs, config,
                            paras);
                }
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isNumericAttr(field, config, paras);
                }
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isBooleanAttr(field, amAttrsConfig, paras);
                }
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isCategoricalAttr(field, config, paras);
                }
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isAttrToRetain(field, paras);
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException("Fail to classify attributes", ex);
        }
    }

    private String findDCVersionToProfile(ProfileConfig config) {
        String dataCloudVersion = config.getDataCloudVersion();
        if (dataCloudVersion == null) {
            switch (config.getStage()) {
            case DataCloudConstants.PROFILE_STAGE_SEGMENT:
                dataCloudVersion = dataCloudVersionService.currentApprovedVersion().getVersion();
                break;
            case DataCloudConstants.PROFILE_STAGE_ENRICH:
                dataCloudVersion = dataCloudVersionService
                        .nextMinorVersion(dataCloudVersionService.currentApprovedVersion().getVersion());
                break;
            default:
                throw new UnsupportedOperationException(String.format("Stage %s is not supported", config.getStage()));
            }
        }
        log.info("Profiling is based on datacloud version " + dataCloudVersion);
        return dataCloudVersion;
    }

    private Map<String, ProfileArgument> findAMAttrsConfig(ProfileConfig config, String dataCloudVersion) {
        List<SourceAttribute> srcAttrs = srcAttrEntityMgr.getAttributes(AM_PROFILE, config.getStage(),
                config.getTransformer(), dataCloudVersion);
        if (CollectionUtils.isEmpty(srcAttrs)) {
            throw new RuntimeException("Fail to find configuration for profiling in SourceAttribute table");
        }
        Map<String, ProfileArgument> amAttrsConfig = new HashMap<>();
        srcAttrs.forEach(attr -> amAttrsConfig.put(attr.getAttribute(),
                JsonUtils.deserialize(attr.getArguments(), ProfileArgument.class)));
        return amAttrsConfig;
    }

    private Schema findSchema(Source baseSrc, String baseVer) {
        Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(baseSrc, baseVer);
        if (schema == null) {
            String avroGlob = TransformerUtils.avroPath(baseSrc, baseVer, hdfsPathBuilder);
            schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        }
        return schema;
    }

    /**
     * @param amAttrsConfig:
     *            attr name -> profile argument
     * @param encAttrsMap:
     *            encoded attr -> [decoded attrs] (Enabled in profiling)
     * @param encAttrs:
     *            all encoded attrs (enabled & disabled in profiling)
     * @param decAttrsMap:
     *            decoded attr name -> decoded attr object
     * @param decodeStrs:
     *            decoded attr -> decode strategy str
     * @throws JsonProcessingException
     * @throws IOException
     */
    private void parseEncodedAttrsConfig(Map<String, ProfileArgument> amAttrsConfig,
            Map<String, List<ProfileParameters.Attribute>> encAttrsMap, Set<String> encAttrs,
            Map<String, ProfileParameters.Attribute> decAttrsMap, Map<String, String> decodeStrs)
            throws JsonProcessingException, IOException {
        for (Map.Entry<String, ProfileArgument> amAttrConfig : amAttrsConfig.entrySet()) {
            if (!AttrClassifier.isEncodedAttr(amAttrConfig.getValue())) {
                continue;
            }
            BitDecodeStrategy decodeStrategy = amAttrConfig.getValue().getDecodeStrategy();
            String decodeStrategyStr = JsonUtils.serialize(decodeStrategy);
            String encAttr = decodeStrategy.getEncodedColumn();
            encAttrs.add(encAttr);
            if (!AttrClassifier.isProfileEnabled(amAttrConfig.getValue())) {
                continue;
            }
            if (!encAttrsMap.containsKey(encAttr)) {
                encAttrsMap.put(encAttr, new ArrayList<>());
            }
            validateEncodedSrcAttrArg(amAttrConfig.getKey(), amAttrConfig.getValue());
            BucketAlgorithm bktAlgo = parseBucketAlgo(amAttrConfig.getValue().getBktAlgo(),
                    decodeStrategy.getValueDict());
            Integer numBits = amAttrConfig.getValue().getNumBits() != null ? amAttrConfig.getValue().getNumBits()
                    : decideBitNumFromBucketAlgo(bktAlgo);
            ProfileParameters.Attribute attr = new ProfileParameters.Attribute(amAttrConfig.getKey(), numBits,
                    decodeStrategyStr, bktAlgo);
            encAttrsMap.get(encAttr).add(attr);
            decAttrsMap.put(attr.getAttr(), attr);
            decodeStrs.put(amAttrConfig.getKey(), decodeStrategyStr);
        }
    }

    private void validateEncodedSrcAttrArg(String attrName, ProfileArgument arg) {
        if (arg.getBktAlgo() == null) {
            throw new RuntimeException(String.format("Please provide BktAlgo for attribute %s", attrName));
        }
    }

    private Integer decideBitNumFromBucketAlgo(BucketAlgorithm algo) {
        if (algo instanceof BooleanBucket) {
            return 2;
        } else if (algo instanceof CategoricalBucket) {
            return Math.max(
                    (int) Math.ceil(Math.log(((CategoricalBucket) algo).getCategories().size() + 1) / Math.log(2)), 1);
        } else if (algo instanceof DiscreteBucket) {
            return Math.max((int) Math.ceil(Math.log(((DiscreteBucket) algo).getValues().size() + 1) / Math.log(2)), 1);
        } else if (algo instanceof IntervalBucket) {
            return null;
        }
        throw new UnsupportedOperationException(
                String.format("Unsupported bucket algorithm for encoded attribute: %s", algo.toString()));
    }

    private BucketAlgorithm parseBucketAlgo(String algo, String valueDict) {
        if (StringUtils.isBlank(algo)) {
            return null;
        }
        if (BooleanBucket.class.getSimpleName().equalsIgnoreCase(algo)) {
            return new BooleanBucket();
        }
        if (CategoricalBucket.class.getSimpleName().equalsIgnoreCase(algo)) {
            if (StringUtils.isBlank(valueDict)) {
                throw new RuntimeException("Value dict is missing for categorical bucket");
            }
            CategoricalBucket bucket = new CategoricalBucket();
            String[] valueDictArr = valueDict.split("\\|\\|");
            bucket.setCategories(new ArrayList<String>(Arrays.asList(valueDictArr)));
            return bucket;
        }
        if (IntervalBucket.class.getSimpleName().equalsIgnoreCase(algo)) {
            return new IntervalBucket();
        }
        if (CategorizedIntervalBucket.class.getSimpleName().equalsIgnoreCase(algo)) {
            return new CategorizedIntervalBucket();
        }
        throw new RuntimeException(String.format("Fail to cast %s to BucketAlgorithm", algo));
    }

    private void postProcessProfiledAttrs(String avroDir, ProfileParameters paras) {
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, avroDir + "/*.avro");
        Map<String, Attribute> numericAttrsMap = new HashMap<>(); // attr name -> attr
        paras.getNumericAttrs().forEach(numericAttr -> numericAttrsMap.put(numericAttr.getAttr(), numericAttr));
        Map<String, Attribute> catAttrsMap = new HashMap<>();    // attr name -> attr
        paras.getCatAttrs().forEach(catAttr -> catAttrsMap.put(catAttr.getAttr(), catAttr));
        for (GenericRecord record : records) {
            String attrName = record.get(DataCloudConstants.PROFILE_ATTR_ATTRNAME).toString();
            boolean readyForNext = false;
            readyForNext = AttrClassifier.isNoBucketAttr(attrName, record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO),
                    paras, numericAttrsMap, catAttrsMap);
            if (!readyForNext) {
                readyForNext = AttrClassifier.isIntervalBucketAttr(attrName,
                        record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO), numericAttrsMap);
            }
            if (!readyForNext) {
                readyForNext = AttrClassifier.isDiscreteBucketAttr(attrName,
                        record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO), numericAttrsMap);
            }
            if (!readyForNext) {
                readyForNext = AttrClassifier.isCategoricalBucketAttr(attrName,
                        record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO), catAttrsMap);
            }
            if (!readyForNext) {
                throw new RuntimeException(String.format("Unknown bucket algorithm for attribute %s: %s", attrName,
                        record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO).toString()));
            }
        }
        // Only for ENRICHMENT case, so all the attributes are from AM
        paras.getAmAttrsToEnc().addAll(paras.getNumericAttrs());
        paras.getAmAttrsToEnc().addAll(paras.getCatAttrs());
        try {
            List<String> avros = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir, ".*\\.avro$");
            for (String path : avros) {
                HdfsUtils.rmdir(yarnConfiguration, path);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete " + avroDir + "/*.avro", e);
        }
    }

    private void reviewFinalAttrGroup(List<Object[]> result,
            Map<String, List<ProfileParameters.Attribute>> amAttrsToEnc,
            Map<String, List<ProfileParameters.Attribute>> exAttrsToEnc, ProfileConfig config,
            ProfileParameters paras) {
        int size = result.size() + paras.getNumericAttrs().size() + amAttrsToEnc.size() + exAttrsToEnc.size();
        switch (config.getStage()) {
        case DataCloudConstants.PROFILE_STAGE_ENRICH:
            log.info(String.format(
                    "%d numeric attrs(groupd into encode attrs and retain attrs), %d am attrs to encode, %d external attrs to encode, %d attrs to retain",
                    paras.getNumericAttrs().size(), amAttrsToEnc.size(), exAttrsToEnc.size(),
                    paras.getAttrsToRetain().size()));
            break;
        case DataCloudConstants.PROFILE_STAGE_SEGMENT:
            log.info(String.format(
                    "%d numeric attrs, %d am attrs to encode, %d external attrs to encode, %d attrs to retain",
                    paras.getNumericAttrs().size(), amAttrsToEnc.size(), exAttrsToEnc.size(),
                    paras.getAttrsToRetain().size()));
            break;
        default:
            throw new RuntimeException("Unrecognized stage " + config.getStage());
        }
        if (config.getStage().equals(DataCloudConstants.PROFILE_STAGE_SEGMENT) && size > maxAttrs) {
            log.warn(String.format("Attr num after bucket and encode is %d, exceeding expected maximum limit %d", size,
                    maxAttrs));
        } else {
            log.info(String.format("Attr num after bucket and encode: %d", size));
        }
    }

    private List<Pair<String, Class<?>>> prepareColumns() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(DataCloudConstants.PROFILE_ATTR_ATTRNAME, String.class));
        columns.add(Pair.of(DataCloudConstants.PROFILE_ATTR_SRCATTR, String.class));
        columns.add(Pair.of(DataCloudConstants.PROFILE_ATTR_DECSTRAT, String.class));
        columns.add(Pair.of(DataCloudConstants.PROFILE_ATTR_ENCATTR, String.class));
        columns.add(Pair.of(DataCloudConstants.PROFILE_ATTR_LOWESTBIT, Integer.class));
        columns.add(Pair.of(DataCloudConstants.PROFILE_ATTR_NUMBITS, Integer.class));
        columns.add(Pair.of(DataCloudConstants.PROFILE_ATTR_BKTALGO, String.class));
        return columns;
    }

    private void uploadAvro(String targetDir, List<Pair<String, Class<?>>> schema, Object[][] data) {
        try {
            AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, targetDir, "Profile.avro");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Object[] profileIdAttr(String idAttr) {
        Object[] data = new Object[7];
        data[0] = DataCloudConstants.LATTICE_ACCOUNT_ID;
        data[1] = idAttr;
        data[2] = null;
        data[3] = null;
        data[4] = null;
        data[5] = null;
        data[6] = null;
        return data;
    }

    private Object[] profileAttrToRetain(ProfileParameters.Attribute attr) {
        Object[] data = new Object[7];
        data[0] = attr.getAttr();
        data[1] = attr.getAttr();
        data[2] = attr.getDecodeStrategy();
        data[3] = null;
        data[4] = null;
        data[5] = null;
        data[6] = attr.getAlgo() == null ? null : JsonUtils.serialize(attr.getAlgo());
        return data;
    }

    private Object[] profileAttrToEnc(ProfileParameters.Attribute attr, String encodedAttr, int lowestBit) {
        Object[] data = new Object[7];
        data[0] = attr.getAttr();
        data[1] = attr.getAttr();
        data[2] = attr.getDecodeStrategy();
        data[3] = encodedAttr;
        data[4] = lowestBit;
        data[5] = attr.getEncodeBitUnit();
        data[6] = attr.getAlgo() == null ? null : JsonUtils.serialize(attr.getAlgo());
        return data;
    }

    private Map<String, List<ProfileParameters.Attribute>> groupAttrsToEnc(List<ProfileParameters.Attribute> attrs,
            String encAttrPrefix) {
        Map<String, List<ProfileParameters.Attribute>> encodedAttrs = new HashMap<>();
        if (CollectionUtils.isEmpty(attrs)) {
            return encodedAttrs;
        }
        attrs.sort((x, y) -> y.getEncodeBitUnit().compareTo(x.getEncodeBitUnit())); // descending order
        List<Map<String, List<ProfileParameters.Attribute>>> availableBits = new ArrayList<>(); // 0 - encodeBits-1
        for (int i = 0; i < encodeBits; i++) {
            availableBits.add(new HashMap<>());
        }
        int encodedSeq = 0;
        for (ProfileParameters.Attribute attr : attrs) {
            if (attr.getEncodeBitUnit() == null || attr.getEncodeBitUnit() <= 0
                    || attr.getEncodeBitUnit() > encodeBits) {
                throw new RuntimeException(String.format("Attribute %s EncodeBitUnit %d is not in range [1, %d]",
                        attr.getAttr(), attr.getEncodeBitUnit(), encodeBits));
            }
            int index = attr.getEncodeBitUnit();
            while (index < encodeBits && availableBits.get(index).size() == 0) {
                index++;
            }
            String encodedAttr = null;
            List<ProfileParameters.Attribute> attachedAttrs = null;
            if (index == encodeBits) { // No available encode attr to add this
                                       // attr. Add a new encode attr
                encodedAttr = createEncodeAttrName(encAttrPrefix, encodedSeq);
                encodedSeq++;
                attachedAttrs = new ArrayList<>();
            } else { // find available encode attr to add this attr
                encodedAttr = availableBits.get(index).entrySet().iterator().next().getKey();
                attachedAttrs = availableBits.get(index).get(encodedAttr);
                availableBits.get(index).remove(encodedAttr);
            }
            attachedAttrs.add(attr);
            availableBits.get(index - attr.getEncodeBitUnit()).put(encodedAttr, attachedAttrs);
        }
        for (Map<String, List<ProfileParameters.Attribute>> entry : availableBits) {
            entry.forEach(encodedAttrs::putIfAbsent);
        }
        return encodedAttrs;
    }

    private String createEncodeAttrName(String encAttrPrefix, int encodedSeq) {
        return encAttrPrefix + String.valueOf(encodedSeq);
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ProfileArgument {
        @JsonProperty("IsProfile")
        private Boolean isProfile = false;

        @JsonProperty("DecodeStrategy")
        private BitDecodeStrategy decodeStrategy;

        @JsonProperty("BktAlgo")
        private String bktAlgo;

        @JsonProperty("NoBucket")
        private Boolean noBucket = false;

        @JsonProperty("NumBits")
        private Integer numBits;

        public Boolean isProfile() {
            return isProfile;
        }

        public BitDecodeStrategy getDecodeStrategy() {
            return decodeStrategy;
        }

        public String getBktAlgo() {
            return bktAlgo;
        }

        public Boolean isNoBucket() {
            return noBucket;
        }

        public Integer getNumBits() {
            return numBits;
        }
    }


    private static class AttrClassifier {

        private static boolean isEncodedAttr(ProfileArgument arg) {
            if (arg.decodeStrategy == null) {
                return false;
            } else {
                return true;
            }
        }

        private static boolean isProfileEnabled(ProfileArgument arg) {
            return arg.isProfile;
        }

        private static boolean isIdAttr(Field field, ProfileParameters paras) {
            if (field.name().equals(DataCloudConstants.LATTIC_ID)
                    || field.name().equals(DataCloudConstants.LATTICE_ACCOUNT_ID)) {
                if (paras.getIdAttr() != null) {
                    throw new RuntimeException("Only allow one ID field (LatticeAccountId or LatticeID)");
                }
                log.info(String.format("ID attr: %s (unencode)", field.name()));
                paras.setIdAttr(field.name());
                return true;
            } else {
                return false;
            }
        }

        private static boolean isAttrToDiscard(Field field, Map<String, ProfileArgument> amAttrConfig) {
            if (!amAttrConfig.containsKey(field.name())) {
                return false;
            }
            if (Boolean.FALSE.equals(amAttrConfig.get(field.name()).isProfile)) {
                log.info(String.format("Discarded attr: %s", field.name()));
                return true;
            }
            return false;
        }

        private static boolean isAttrNoBucket(Field field, Map<String, ProfileArgument> amAttrConfig,
                ProfileParameters paras)
                throws JsonProcessingException, IOException {
            if (!amAttrConfig.containsKey(field.name())) {
                return false;
            }
            if (Boolean.FALSE.equals(amAttrConfig.get(field.name()).isNoBucket())) {
                return false;
            }
            log.info(String.format("Retained attr: %s (unencode)", field.name()));
            paras.getAttrsToRetain().add(new ProfileParameters.Attribute(field.name(), null, null, null));
            return true;
        }

        private static boolean isPreknownAttr(Field field, Map<String, List<ProfileParameters.Attribute>> encAttrsMap,
                Map<String, ProfileParameters.Attribute> decAttrsMap, Set<String> encAttrs, ProfileConfig config,
                ProfileParameters paras) {
            // Preknown attributes which are encoded (usually for Enrichment use
            // case, since profiled source is AM)
            if (encAttrsMap.containsKey(field.name())) {
                for (ProfileParameters.Attribute attr : encAttrsMap.get(field.name())) {
                    classifyPreknownAttr(attr, config.getStage(), paras.getNumericAttrs(), paras.getCatAttrs(),
                            paras.getAmAttrsToEnc(), paras.getAttrsToRetain());
                }
                return true;
            }
            // Preknown attributes which are in plain format (usually for
            // Segment use case, since profiled source is match result)
            if (decAttrsMap.containsKey(field.name())) {
                classifyPreknownAttr(decAttrsMap.get(field.name()), config.getStage(), paras.getNumericAttrs(),
                        paras.getCatAttrs(), paras.getAmAttrsToEnc(), paras.getAttrsToRetain());
                return true;
            }
            if (encAttrs.contains(field.name())) {
                log.info(String.format(
                        "Ignore encoded attr: %s (No decoded attrs of it are enabled in profiling)",
                        field.name()));
                return true;
            }
            return false;
        }

        private static boolean isNumericAttr(Field field, ProfileConfig config, ProfileParameters paras) {
            Schema.Type type = field.schema().getTypes().get(0).getType();
            if (NUM_TYPES.contains(type)) {
                switch (config.getStage()) {
                case DataCloudConstants.PROFILE_STAGE_SEGMENT:
                    log.info(String.format("Interval bucketed attr %s (type %s unencode)", field.name(),
                            type.getName()));
                    break;
                case DataCloudConstants.PROFILE_STAGE_ENRICH:
                    log.info(String.format("Interval bucketed attr %s (type %s encode)", field.name(), type.getName()));
                    break;
                default:
                    throw new RuntimeException("Unrecognized stage " + config.getStage());
                }
                paras.getNumericAttrs()
                        .add(new ProfileParameters.Attribute(field.name(), null, null, new IntervalBucket()));
                return true;
            }
            return false;
        }

        private static boolean isBooleanAttr(Field field, Map<String, ProfileArgument> amAttrConfig,
                ProfileParameters paras) {
            Schema.Type type = field.schema().getTypes().get(0).getType();
            if (BOOL_TYPES.contains(type)
                    || FundamentalType.BOOLEAN.getName().equalsIgnoreCase(field.getProp(AVRO_PROP_KEY))) {
                log.info(String.format("Boolean bucketed attr %s (type %s encode)", field.name(), type.getName()));
                BucketAlgorithm algo = new BooleanBucket();
                if (amAttrConfig.containsKey(field.name())) {
                    paras.getAmAttrsToEnc().add(new ProfileParameters.Attribute(field.name(), 2, null, algo));
                } else {
                    paras.getExAttrsToEnc().add(new ProfileParameters.Attribute(field.name(), 2, null, algo));
                }
                return true;
            }
            return false;
        }

        private static boolean isCategoricalAttr(Field field, ProfileConfig config, ProfileParameters paras) {
            Schema.Type type = field.schema().getTypes().get(0).getType();
            if (CAT_TYPES.contains(type)) {
                switch (config.getStage()) {
                case DataCloudConstants.PROFILE_STAGE_SEGMENT:
                    log.info(String.format("Categorical bucketed attr %s (type %s unencode)", field.name(),
                            type.getName()));
                    break;
                case DataCloudConstants.PROFILE_STAGE_ENRICH:
                    log.info(String.format("Categorical bucketed attr %s (type %s encode)", field.name(),
                            type.getName()));
                    break;
                default:
                    throw new RuntimeException("Unrecognized stage " + config.getStage());
                }
                paras.getCatAttrs()
                        .add(new ProfileParameters.Attribute(field.name(), null, null, new CategoricalBucket()));
                return true;
            }
            return false;
        }

        private static boolean isAttrToRetain(Field field, ProfileParameters paras) {
            log.info(String.format("Retained attr: %s (unencode)", field.name()));
            paras.getAttrsToRetain().add(new ProfileParameters.Attribute(field.name(), null, null, null));
            return true;
        }
        
        private static void classifyPreknownAttr(ProfileParameters.Attribute attr, String stage,
                List<Attribute> numericAttrs,
                List<Attribute> catAttrs, List<Attribute> amAttrsToEnc, List<Attribute> attrsToRetain) {
            switch (stage) {
            case DataCloudConstants.PROFILE_STAGE_SEGMENT:
                if (attr.getAlgo() instanceof BooleanBucket || attr.getAlgo() instanceof CategoricalBucket) {
                    log.info(String.format("%s attr %s (encode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    amAttrsToEnc.add(attr);
                } else if (attr.getAlgo() instanceof IntervalBucket) {
                    log.info(String.format("%s attr %s (unencode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    numericAttrs.add(attr);
                } else {
                    log.info(String.format("%s attr %s (unencode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    attrsToRetain.add(attr);
                }
                break;
            case DataCloudConstants.PROFILE_STAGE_ENRICH:
                if (attr.getAlgo() instanceof BooleanBucket || attr.getAlgo() instanceof CategoricalBucket) {
                    log.info(String.format("%s attr %s (encode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    amAttrsToEnc.add(attr);
                } else if (attr.getAlgo() instanceof IntervalBucket) {
                    log.info(String.format("%s attr %s (encode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    numericAttrs.add(attr);
                } else {
                    log.info(String.format("%s attr %s (unencode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    attrsToRetain.add(attr);
                }
                break;
            default:
                throw new RuntimeException("Unrecognized stage " + stage);
            }
        }

        private static boolean isNoBucketAttr(String attrName, Object bucketAlgo, ProfileParameters paras,
                Map<String, Attribute> numericAttrsMap, Map<String, Attribute> catAttrsMap) {
            Set<String> catAttrsNotEnc = paras.getCatAttrsNotEnc() != null
                    ? new HashSet<>(Arrays.asList(paras.getCatAttrsNotEnc()))
                    : new HashSet<>();
            if (bucketAlgo != null && !catAttrsNotEnc.contains(attrName)) {
                return false;
            }
            if (numericAttrsMap.containsKey(attrName)) {
                numericAttrsMap.get(attrName).setAlgo(null);
                paras.getAttrsToRetain().add(numericAttrsMap.get(attrName));
                paras.getNumericAttrs().remove(numericAttrsMap.get(attrName));
                log.warn(String.format(
                        "Attribute %s is moved from encode numeric group to retained group due to all the values are null: %s",
                        attrName, JsonUtils.serialize(numericAttrsMap.get(attrName))));
                return true;
            }
            if (catAttrsMap.containsKey(attrName)) {
                catAttrsMap.get(attrName).setAlgo(null);
                paras.getAttrsToRetain().add(catAttrsMap.get(attrName));
                paras.getCatAttrs().remove(catAttrsMap.get(attrName));
                log.warn(String.format(
                        "Attribute %s is moved from encode categorical group to retained group due to there is no buckets or it is a dimensional attribute for stats calculation: (%s) ",
                        attrName, JsonUtils.serialize(numericAttrsMap.get(attrName))));
                return true;
            }
            throw new RuntimeException(String.format(
                    "Attribute %s in profiled result has null bucket algorithm and does not belong to either numerical attribute or categorical attribute",
                    attrName));
        }

        private static boolean isIntervalBucketAttr(String attrName, Object bucketAlgo,
                Map<String, Attribute> numericAttrsMap) {
            try {
                IntervalBucket algo = JsonUtils.deserialize(bucketAlgo.toString(), IntervalBucket.class);
                if (!numericAttrsMap.containsKey(attrName)) {
                    throw new RuntimeException(
                            String.format("Fail to find attribute %s with IntervalBucket %s in numeric attribute list",
                                    attrName, bucketAlgo.toString()));
                }
                numericAttrsMap.get(attrName).setAlgo(algo);
                // reason for boundary+2: 1 for catNum = boundNum + 1; 1 for null
                Integer numBits = Math.max((int) Math.ceil(Math.log((algo.getBoundaries().size() + 2)) / Math.log(2)),
                        1);
                numericAttrsMap.get(attrName).setEncodeBitUnit(numBits);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        private static boolean isDiscreteBucketAttr(String attrName, Object bucketAlgo,
                Map<String, Attribute> numericAttrsMap) {
            try {
                DiscreteBucket algo = JsonUtils.deserialize(bucketAlgo.toString(), DiscreteBucket.class);
                if (!numericAttrsMap.containsKey(attrName)) {
                    throw new RuntimeException(
                            String.format("Fail to find attribute %s with DiscreteBucket %s in numeric attribute list",
                                    attrName, bucketAlgo.toString()));
                }
                numericAttrsMap.get(attrName).setAlgo(algo);
                Integer numBits = Math.max((int) Math.ceil(Math.log((algo.getValues().size() + 1)) / Math.log(2)), 1);
                numericAttrsMap.get(attrName).setEncodeBitUnit(numBits);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        private static boolean isCategoricalBucketAttr(String attrName, Object bucketAlgo,
                Map<String, Attribute> catAttrsMap) {
            try {
                CategoricalBucket algo = JsonUtils.deserialize(bucketAlgo.toString(), CategoricalBucket.class);
                if (!catAttrsMap.containsKey(attrName)) {
                    throw new RuntimeException(String.format(
                            "Fail to find attribute %s with CategoricalBucket %s in categorical attribute list",
                            attrName, bucketAlgo.toString()));
                }
                catAttrsMap.get(attrName).setAlgo(algo);
                Integer numBits = Math.max((int) Math.ceil(Math.log((algo.getCategories().size() + 1)) / Math.log(2)),
                        1);
                catAttrsMap.get(attrName).setEncodeBitUnit(numBits);
                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }
}
