package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceProfiler.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.metadata.FundamentalType.AVRO_PROP_KEY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
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
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.ProfileParameters.Attribute;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.metadata.FundamentalType;


@Component(TRANSFORMER_NAME)
public class SourceProfiler extends AbstractDataflowTransformer<ProfileConfig, ProfileParameters> {
    private static final Logger log = LoggerFactory.getLogger(SourceProfiler.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_PROFILER;
    public static final String AM_PROFILE = "AMProfile";
    public static final String IS_PROFILE = "IsProfile";
    public static final String DECODE_STRATEGY = "DecodeStrategy";
    public static final String ENCODED_COLUMN = "EncodedColumn";
    public static final String NUM_BITS = "NumBits";
    public static final String BKT_ALGO = "BktAlgo";
    public static final String VALUE_DICT = "ValueDict";

    @Value("${datacloud.etl.profile.encode.bit:64}")
    private int encodeBits;

    @Value("${datacloud.etl.profile.attrs:1000}")
    private int maxAttrs;

    private ObjectMapper om = new ObjectMapper();

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    SourceAttributeEntityMgr srcAttrEntityMgr;

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
        List<String> idAttrs = new ArrayList<>();
        List<ProfileParameters.Attribute> numericAttrs = new ArrayList<>();
        List<ProfileParameters.Attribute> catAttrs = new ArrayList<>();
        List<ProfileParameters.Attribute> attrsToRetain = new ArrayList<>();
        List<ProfileParameters.Attribute> amAttrsToEnc = new ArrayList<>();
        List<ProfileParameters.Attribute> exAttrsToEnc = new ArrayList<>();
        Map<String, BitCodeBook> codeBookMap = new HashMap<>();
        Map<String, String> codeBookLookup = new HashMap<>();
        classifyAttrs(step.getBaseSources()[0], step.getBaseVersions().get(0), config, idAttrs, numericAttrs, catAttrs,
                attrsToRetain, amAttrsToEnc, exAttrsToEnc, codeBookMap, codeBookLookup);
        if (CollectionUtils.isEmpty(idAttrs)) {
            log.warn("Cannot find ID field (LatticeAccountId, LatticeID).");
        }
        if (idAttrs.size() > 1) {
            throw new RuntimeException("Only allow one ID field (LatticeAccountId or LatticeID)");
        }
        paras.setNumBucketEqualSized(config.isNumBucketEqualSized());
        paras.setBucketNum(config.getBucketNum());
        paras.setMinBucketSize(config.getMinBucketSize());
        paras.setRandSeed(config.getRandSeed());
        paras.setEncAttrPrefix(config.getEncAttrPrefix());
        paras.setMaxCats(config.getMaxCat());
        paras.setCatAttrsNotEnc(config.getCatAttrsNotEnc());
        paras.setIdAttr(idAttrs.get(0));
        paras.setNumericAttrs(numericAttrs);
        paras.setCatAttrs(catAttrs);
        paras.setAttrsToRetain(attrsToRetain);
        paras.setAmAttrsToEnc(amAttrsToEnc);
        paras.setExAttrsToEnc(exAttrsToEnc);
        paras.setCodeBookMap(codeBookMap);
        paras.setCodeBookLookup(codeBookLookup);
    }

    private void classifyAttrs(Source baseSrc, String baseVer, ProfileConfig config, List<String> idAttrs,
            List<ProfileParameters.Attribute> numericAttrs, List<ProfileParameters.Attribute> catAttrs,
            List<ProfileParameters.Attribute> attrsToRetain, List<ProfileParameters.Attribute> amAttrsToEnc,
            List<ProfileParameters.Attribute> exAttrsToEnc, Map<String, BitCodeBook> codeBookMap,
            Map<String, String> codeBookLookup) {
        List<SourceAttribute> srcAttrs = srcAttrEntityMgr.getAttributes(AM_PROFILE, config.getStage(),
                config.getTransformer());
        Map<String, SourceAttribute> amAttrConfig = new HashMap<>();    // attr name -> srcAttr
        srcAttrs.forEach(attr -> amAttrConfig.put(attr.getAttribute(), attr));

        Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(baseSrc, baseVer);
        if (schema == null) {
            String avroGlob = TransformerUtils.avroPath(baseSrc, baseVer, hdfsPathBuilder);
            schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        }

        log.info("Classifying attributes...");
        Set<Schema.Type> numTypes = new HashSet<>(Arrays.asList(
                new Schema.Type[] { Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE }));
        Set<Schema.Type> boolTypes = new HashSet<>(Arrays.asList(new Schema.Type[] { Schema.Type.BOOLEAN }));
        Set<Schema.Type> catTypes = new HashSet<>(Arrays.asList(new Schema.Type[] { Schema.Type.STRING }));
        try {
            // Attributes encoded in the profiled source which need to decode
            Map<String, List<ProfileParameters.Attribute>> encAttrMap = new HashMap<>(); // Encoded attr-> [decoded attrs] (Enabled in profiling)
            Set<String> encAttrs = new HashSet<>(); // All encoded attrs (enabled/disabled in profiling)
            Map<String, String> decodeStrs = new HashMap<>();   // decoded attr -> decode strategy str
            for (SourceAttribute amAttr : amAttrConfig.values()) {
                JsonNode arg = om.readTree(amAttr.getArguments());
                if (!arg.hasNonNull(IS_PROFILE)) {
                    throw new RuntimeException(
                            String.format("Please provide IsProfile flag for attribute %s", amAttr.getAttribute()));
                }
                if (arg.get(DECODE_STRATEGY) == null) {
                    continue;
                }
                String decodeStrategyStr = arg.get(DECODE_STRATEGY).toString();
                BitDecodeStrategy decodeStrategy = JsonUtils.deserialize(decodeStrategyStr, BitDecodeStrategy.class);
                String encAttr = decodeStrategy.getEncodedColumn();
                encAttrs.add(encAttr);
                if (!arg.get(IS_PROFILE).asBoolean()) {
                    continue;
                }
                if (!encAttrMap.containsKey(encAttr)) {
                    encAttrMap.put(encAttr, new ArrayList<>());
                }
                Integer numBits = arg.has(NUM_BITS) ? arg.get(NUM_BITS).asInt() : null;
                if (!arg.hasNonNull(BKT_ALGO)) {
                    throw new RuntimeException(
                            String.format("Please provide BktAlgo for attribute %s", amAttr.getAttribute()));
                }
                BucketAlgorithm bktAlgo = parseBucketAlgo(arg.get(BKT_ALGO).asText(), decodeStrategy.getValueDict());
                if (numBits == null) {
                    if (bktAlgo instanceof BooleanBucket) {
                        numBits = 2;
                    } else if (bktAlgo instanceof CategoricalBucket) {
                        numBits = Math.max((int) Math
                                .ceil(Math.log(((CategoricalBucket) bktAlgo).getCategories().size() + 1) / Math.log(2)),
                                1);
                    }
                }
                encAttrMap.get(encAttr).add(
                        new ProfileParameters.Attribute(amAttr.getAttribute(), numBits, decodeStrategyStr, bktAlgo));
                decodeStrs.put(amAttr.getAttribute(), decodeStrategyStr);
            }
            // Build BitCodeBook
            BitCodeBookUtils.constructCodeBookMap(codeBookMap, codeBookLookup, decodeStrs);
            // Attributes exist in the profiled source
            for (Field field : schema.getFields()) {
                if (field.name().equals(DataCloudConstants.LATTIC_ID)
                        || field.name().equals(DataCloudConstants.LATTICE_ACCOUNT_ID)) {
                    log.info(String.format("ID attr: %s (unencode)", field.name()));
                    idAttrs.add(field.name());
                    continue;
                }
                boolean isProfile = true;
                if (amAttrConfig.containsKey(field.name())) {
                    JsonNode arg = om.readTree(amAttrConfig.get(field.name()).getArguments());
                    isProfile = arg.get(IS_PROFILE).asBoolean();
                }
                if (!isProfile) {
                    log.info(String.format("Discarded attr: %s", field.name()));
                    continue;
                }
                if (encAttrMap.containsKey(field.name())) {
                    for (ProfileParameters.Attribute attr : encAttrMap.get(field.name())) {
                        switch (config.getStage()) {
                        case DataCloudConstants.PROFILE_STAGE_SEGMENT:
                            if (attr.getAlgo() instanceof BooleanBucket) {
                                log.info(String.format("%s attr %s (encode)", attr.getAlgo().getClass().getSimpleName(),
                                        attr.getAttr()));
                                amAttrsToEnc.add(attr); // No external attrs needs to decode
                            } else if (attr.getAlgo() instanceof IntervalBucket) {
                                log.info(String.format("%s attr %s (unencode)",
                                        attr.getAlgo().getClass().getSimpleName(),
                                        attr.getAttr()));
                                numericAttrs.add(attr);
                            } else {
                                log.info(String.format("%s attr %s (unencode)",
                                        attr.getAlgo().getClass().getSimpleName(),
                                        attr.getAttr()));
                                attrsToRetain.add(attr);
                            }
                            break;
                        case DataCloudConstants.PROFILE_STAGE_ENRICH:
                            if (attr.getAlgo() instanceof BooleanBucket
                                    || attr.getAlgo() instanceof CategoricalBucket) {
                                log.info(String.format("%s attr %s (encode)",
                                        attr.getAlgo().getClass().getSimpleName(), attr.getAttr()));
                                amAttrsToEnc.add(attr);
                            } else if (attr.getAlgo() instanceof IntervalBucket) {
                                log.info(String.format("%s attr %s (encode)",
                                        attr.getAlgo().getClass().getSimpleName(), attr.getAttr()));
                                numericAttrs.add(attr);
                            } else {
                                log.info(String.format("%s attr %s (unencode)",
                                        attr.getAlgo().getClass().getSimpleName(), attr.getAttr()));
                                attrsToRetain.add(attr);
                            }
                            break;
                        default:
                            throw new RuntimeException("Unrecognized stage " + config.getStage());
                        }
                    }
                    continue;
                }
                Schema.Type type = field.schema().getTypes().get(0).getType();
                if (numTypes.contains(type)) {
                    switch (config.getStage()) {
                    case DataCloudConstants.PROFILE_STAGE_SEGMENT:
                        log.info(String.format("Interval bucketed attr %s (type %s unencode)", field.name(),
                                type.getName()));
                        break;
                    case DataCloudConstants.PROFILE_STAGE_ENRICH:
                        log.info(String.format("Interval bucketed attr %s (type %s encode)", field.name(),
                                type.getName()));
                        break;
                    default:
                        throw new RuntimeException("Unrecognized stage " + config.getStage());
                    }
                    numericAttrs.add(new ProfileParameters.Attribute(field.name(), null, null, new IntervalBucket()));
                    continue;
                }
                if (boolTypes.contains(type)
                        || FundamentalType.BOOLEAN.getName().equalsIgnoreCase(field.getProp(AVRO_PROP_KEY))) {
                    log.info(String.format("Boolean bucketed attr %s (type %s encode)", field.name(), type.getName()));
                    BucketAlgorithm algo = new BooleanBucket();
                    if (amAttrConfig.containsKey(field.name())) {
                        amAttrsToEnc.add(new ProfileParameters.Attribute(field.name(), 2, null, algo));
                    } else {
                        exAttrsToEnc.add(new ProfileParameters.Attribute(field.name(), 2, null, algo));
                    }
                    continue;
                }
                if (encAttrs.contains(field.name())) {
                    log.info(String.format("Existing encoded attr: %s", field.name()));
                    continue;
                }
                if (catTypes.contains(type)) {
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
                    catAttrs.add(new ProfileParameters.Attribute(field.name(), null, null, new CategoricalBucket()));
                    continue;
                }
                log.info(String.format("Retained attr: %s (unencode)", field.name()));
                attrsToRetain.add(new ProfileParameters.Attribute(field.name(), null, null, null));
            }
        } catch (Exception ex) {
            throw new RuntimeException("Fail to classify attributes", ex);
        }
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

    @Override
    protected void postDataFlowProcessing(String workflowDir, ProfileParameters paras, ProfileConfig config) {
        if (config.getStage().equals(DataCloudConstants.PROFILE_STAGE_ENRICH)) {
            postProcessProfiledAttrs(workflowDir, paras);
        }
        List<Object[]> result = new ArrayList<>();
        result.add(profileIdAttr(paras.getIdAttr()));
        for (ProfileParameters.Attribute attr : paras.getAttrsToRetain()) {
            result.add(profileAttrToRetain(attr));
        }
        Map<String, List<ProfileParameters.Attribute>> amAttrsToEnc = groupAttrsToEnc(paras.getAmAttrsToEnc(),
                DataCloudConstants.EAttr);
        Map<String, List<ProfileParameters.Attribute>> exAttrsToEnc = groupAttrsToEnc(paras.getExAttrsToEnc(),
                StringUtils.isBlank(config.getEncAttrPrefix()) ? DataCloudConstants.CEAttr : config.getEncAttrPrefix());
        int size = result.size() + paras.getNumericAttrs().size() + amAttrsToEnc.size() + exAttrsToEnc.size();
        switch (config.getStage()) {
        case DataCloudConstants.PROFILE_STAGE_ENRICH:
            log.info(String.format(
                    "1 LatticeAccountId attr, %d numeric attrs(groupd into encode attrs and retain attrs), %d am attrs to encode, %d external attrs to encode, %d attrs to retain",
                    paras.getNumericAttrs().size(), amAttrsToEnc.size(), exAttrsToEnc.size(),
                    paras.getAttrsToRetain().size()));
            break;
        case DataCloudConstants.PROFILE_STAGE_SEGMENT:
            log.info(String.format(
                    "1 LatticeAccountId attr, %d numeric attrs, %d am attrs to encode, %d external attrs to encode, %d attrs to retain",
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

    private void postProcessProfiledAttrs(String avroDir, ProfileParameters paras) {
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, avroDir + "/*.avro");
        List<Attribute> numericAttrs = paras.getNumericAttrs();
        Map<String, Attribute> numericAttrMap = new HashMap<>(); // attr name -> attr
        numericAttrs.forEach(numericAttr -> numericAttrMap.put(numericAttr.getAttr(), numericAttr));
        List<Attribute> catAttrs = paras.getCatAttrs();
        Map<String, Attribute> catAttrMap = new HashMap<>();    // attr name -> attr
        catAttrs.forEach(catAttr -> catAttrMap.put(catAttr.getAttr(), catAttr));
        Set<String> catAttrsNotEnc = paras.getCatAttrsNotEnc() != null
                ? new HashSet<>(Arrays.asList(paras.getCatAttrsNotEnc())) : new HashSet<>();
        for (GenericRecord record : records) {
            String attrName = record.get(DataCloudConstants.PROFILE_ATTR_ATTRNAME).toString();
            if (numericAttrMap.containsKey(attrName)) {
                IntervalBucket algo = record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO) == null ? null
                        : JsonUtils.deserialize(record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO).toString(),
                                IntervalBucket.class);
                numericAttrMap.get(attrName).setAlgo(algo);
                // boundary+2: 1 for catNum = boundNum + 1; 1 for null
                Integer numBits = algo == null ? null
                        : Math.max((int) Math.ceil(Math.log((algo.getBoundaries().size() + 2)) / Math.log(2)), 1);
                numericAttrMap.get(attrName).setEncodeBitUnit(numBits);
            } else if (catAttrMap.containsKey(attrName)) {
                CategoricalBucket algo = record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO) == null ? null
                        : JsonUtils.deserialize(record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO).toString(),
                                CategoricalBucket.class);
                catAttrMap.get(attrName).setAlgo(algo);
                Integer numBits = algo == null ? null
                        : Math.max((int) Math.ceil(Math.log((algo.getCategories().size() + 1)) / Math.log(2)), 1);
                catAttrMap.get(attrName).setEncodeBitUnit(numBits);
            } else {
                throw new RuntimeException("Unknown attribute: " + attrName);
            }
        }
        Iterator<Attribute> iter = numericAttrs.iterator();
        while (iter.hasNext()) {
            Attribute attr = iter.next();
            if (attr.getAlgo() == null) {
                paras.getAttrsToRetain().add(attr);
                iter.remove();
                log.warn(String.format("Attribute %s is moved from encode numeric group to retained group (%s)",
                        attr.getAttr(), JsonUtils.serialize(attr)));
            }
        }
        paras.getAmAttrsToEnc().addAll(paras.getNumericAttrs());
        iter = catAttrs.iterator();
        while (iter.hasNext()) {
            Attribute attr = iter.next();
            if (attr.getAlgo() == null || catAttrsNotEnc.contains(attr.getAttr())) {
                paras.getAttrsToRetain().add(attr);
                iter.remove();
                log.warn(String.format(
                        "Attribute %s is moved from encode categorical group to retained group due to there is no buckets or it is a dimensional attribute for stats calculation: (%s) ",
                        attr.getAttr(), JsonUtils.serialize(attr)));
            }
        }
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
        try {
            data[6] = attr.getAlgo() == null ? null : om.writeValueAsString(attr.getAlgo());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    String.format("Fail to format %s object to json", attr.getAlgo().getClass().getSimpleName()), e);
        }
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
        try {
            data[6] = attr.getAlgo() == null ? null : om.writeValueAsString(attr.getAlgo());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    String.format("Fail to format %s object to json", attr.getAlgo().getClass().getSimpleName()), e);
        }
        return data;
    }

    private Map<String, List<ProfileParameters.Attribute>> groupAttrsToEnc(List<ProfileParameters.Attribute> attrs,
            String encAttrPrefix) {
        attrs.sort((x, y) -> y.getEncodeBitUnit().compareTo(x.getEncodeBitUnit())); // descending order
        Map<String, List<ProfileParameters.Attribute>> encodedAttrs = new HashMap<>();
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

    @Override
    protected void updateStepCount(TransformStep step, String workflowDir) {
        try {
            Long targetRecords = AvroUtils.count(yarnConfiguration, workflowDir + "/*.avro");
            step.setCount(targetRecords);
        } catch (Exception ex) {
            log.error(String.format("Fail to count records in %s", workflowDir), ex);
        }
    }
}
