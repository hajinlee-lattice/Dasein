package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceProfiler.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.metadata.FundamentalType.AVRO_PROP_KEY;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
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
import com.latticeengines.domain.exposed.datacloud.dataflow.DateBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.ProfileParameters.Attribute;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.FundamentalType;


/**
 * Basic knowledge:
 * config.getStage() == DataCloudConstants.PROFILE_STAGE_ENRICH:
 *      serve AccountMasterStatistics job
 * config.getStage() == DataCloudConstants.PROFILE_STAGE_SEGMENT:
 *      serve ProfileAccount in PA job
 */
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

    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    private SourceAttributeEntityMgr srcAttrEntityMgr;

    @Inject
    private DataCloudVersionService dataCloudVersionService;

    private static final Set<Schema.Type> NUM_TYPES = Sets.newHashSet( //
            Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE);
    private static final Set<Schema.Type> BOOL_TYPES = Collections.singleton(Schema.Type.BOOLEAN);
    private static final Set<Schema.Type> CAT_TYPES = Collections.singleton(Schema.Type.STRING);
    private static final Set<Schema.Type> DATE_TYPES = Collections.singleton(Schema.Type.LONG);

    // List of Attributes classified as date attributes.
    private List<Attribute> dateAttrs;

    // List of Attributes to retain which will be defined in the SourceProfiler class itself, rather than through
    // the Profile data flow.
    private List<Attribute> attrsToRetain;

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

    private List<Attribute> getDateAttrs() {
        return dateAttrs;
    }

    private void setDateAttrs(List<Attribute> dateAttrs) {
        this.dateAttrs = dateAttrs;
    }

    private List<Attribute> getAttrsToRetain() { return attrsToRetain; }

    private void setAttrsToRetain(List<Attribute> attrsToRetain) { this.attrsToRetain = attrsToRetain; }

    /*
     * 1. Before dataflow executed, classify attributes in base source and
     * populate idAttr, numericAttrs, catAttrs, amAttrsToEnc, exAttrsToEnc,
     * attrsToRetain in the ProfileParameters
     */
    /*
     * 2. For attributes which do not need dataflow to profile, eg. we already
     * know what could be the bucket, put them in corresponding attribute
     * list/map in ProfileParameters and bucket algo could be finalized in
     * postDataFlowProcessing
     */
    @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir, ProfileParameters paras,
            ProfileConfig config) {
        initProfileParameters(config, paras);
        // Make sure the evaluation date is set to something.  If it isn't, then set it to the beginning of today
        // at UTC.
        if (config.getEvaluationDateAsTimestamp() == -1) {
            long evalDate = LocalDate.now().atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();
            config.setEvaluationDateAsTimestamp(evalDate);
            log.warn("Evaluation Date not set before SourceProfiler, setting to " + evalDate);
        }
        classifyAttrs(step.getBaseSources()[0], step.getBaseVersions().get(0), config, paras);
    }

    /*
     * After dataflow executed, profile result from dataflow and pre-known
     * prefile result need to combine together
     */
    @Override
    protected void postDataFlowProcessing(TransformStep step, String workflowDir, ProfileParameters paras,
            ProfileConfig config) {
        // config.getStage() == DataCloudConstants.PROFILE_STAGE_ENRICH: serve
        // AccountMasterStatistics job
        // config.getStage() == DataCloudConstants.PROFILE_STAGE_SEGMENT: serve
        // ProfileAccount in PA job
        if (DataCloudConstants.PROFILE_STAGE_ENRICH.equals(config.getStage())) {
            postProcessProfiledAttrs(workflowDir, config, paras);
        }
        List<Object[]> result = new ArrayList<>();
        if (paras.getIdAttr() != null) {
            result.add(profileIdAttr(paras.getIdAttr()));
        }
        for (ProfileParameters.Attribute dateAttr : getDateAttrs()) {
            result.add(profileDateAttr(dateAttr));
        }
        for (ProfileParameters.Attribute attr : getAttrsToRetain()) {
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
        setDateAttrs(new ArrayList<>());
        setAttrsToRetain(new ArrayList<>());

        paras.setNumericAttrs(new ArrayList<>());
        paras.setCatAttrs(new ArrayList<>());
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


    /* Classify an attribute belongs to which scenario: */
    /*- DataCloud ID attr: AccountMasterId */
    /*- Discard attr: attr will not show up in bucketed source */
    /*- No bucket attr: attr will show up in bucketed source and stats, but no bucket created. They are DataCloud attrs which are predefined by PM */
    /*- Pre-known bucket attr: DataCloud attrs whose enum values are pre-known, eg. Intent attributes) */
    /*- Numerical attr */
    /*- Boolean attr */
    /*- Categorical attr */
    /*- Other attr: don't know how to profile it for now, don't create bucket for it */
    /*
     * For AccountMasterStatistics job, we will encode numerical attr and
     * categorical attr, but NO for ProfileAccount job in PA. Because
     * BucketedAccount needs to support decode in Redshift query, but NO for
     * bucketed AccountMaster
     */
    private void classifyAttrs(Source baseSrc, String baseVer, ProfileConfig config, ProfileParameters paras) {
        String dataCloudVersion = findDCVersionToProfile(config);
        Map<String, ProfileArgument> amAttrsConfig = findAMAttrsConfig(config, dataCloudVersion);
        Schema schema = findSchema(baseSrc, baseVer);

        log.info("Classifying attributes...");
        try {
            // Parse configuration for attrs already encoded in the profiled source
            // which need to decode

            // encoded attr -> [decoded attrs] (Enabled in profiling)
            Map<String, List<ProfileParameters.Attribute>> encAttrsMap = new HashMap<>();

            // all encoded attrs (enabled/disabled in profiling)
            Set<String> encAttrs = new HashSet<>();

            // decoded attr name -> decoded attr object
            Map<String, ProfileParameters.Attribute> decAttrsMap = new HashMap<>();

            // decoded attr -> decode strategy str
            Map<String, String> decodeStrs = new HashMap<>();
            parseEncodedAttrsConfig(amAttrsConfig, encAttrsMap, encAttrs, decAttrsMap, decodeStrs);

            // Build BitCodeBook
            BitCodeBookUtils.constructCodeBookMap(paras.getCodeBookMap(), paras.getCodeBookLookup(), decodeStrs);

            // Parse flat attrs in the profiled source
            for (Field field : schema.getFields()) {
                boolean readyForNext;
                readyForNext = AttrClassifier.isIdAttr(field, paras);
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isAttrToDiscard(field, amAttrsConfig);
                }
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isAttrNoBucket(field, amAttrsConfig, getAttrsToRetain());
                }
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isPreknownAttr(field, encAttrsMap, decAttrsMap, encAttrs, config,
                            paras, getAttrsToRetain());
                }
                if (!readyForNext) {
                    readyForNext = AttrClassifier.isDateAttr(field, amAttrsConfig, config, getDateAttrs());
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
                    readyForNext = AttrClassifier.isAttrToRetain(field, getAttrsToRetain());
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException("Fail to classify attributes", ex);
        }
    }

    /**
     * For ProfileAccount job in QA, look for metadata of latest approved datacloud version
     * For AccountMasterStatistics job, look for metadata of the datacloud version which is current in build (next datacloud version)
     */
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

    /**
     * Get profile/decode strategy for DataCloud attrs
     */
    private Map<String, ProfileArgument> findAMAttrsConfig(ProfileConfig config, String dataCloudVersion) {
        List<SourceAttribute> srcAttrs;
        if (DataCloudConstants.PROFILE_STAGE_SEGMENT.equals(config.getStage())) {
            srcAttrs = srcAttrEntityMgr.getAttributes(AM_PROFILE, config.getStage(),
                    config.getTransformer(), dataCloudVersion, true);
        } else {
            srcAttrs = srcAttrEntityMgr.getAttributes(AM_PROFILE, config.getStage(),
                    config.getTransformer(), dataCloudVersion, false);
        }
        if (CollectionUtils.isEmpty(srcAttrs)) {
            throw new RuntimeException("Fail to find configuration for profiling in SourceAttribute table");
        }
        Map<String, ProfileArgument> amAttrsConfig = new HashMap<>();
        srcAttrs.forEach(attr -> amAttrsConfig.put(attr.getAttribute(),
                JsonUtils.deserialize(attr.getArguments(), ProfileArgument.class)));
        return amAttrsConfig;
    }

    /**
     * Get schema for base source to be profiled. Schema file has metadata for
     * customer attrs (non-DataCloud attrs)
     */
    private Schema findSchema(Source baseSrc, String baseVer) {
        Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(baseSrc, baseVer);
        if (schema == null) {
            String avroGlob = TransformerUtils.avroPath(baseSrc, baseVer, hdfsPathBuilder);
            schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        }
        return schema;
    }

    /**
     * Some DataCloud attrs are encoded already. We need to put DecodeStrategy
     * in profile result so that bucket job will know how to decode them. For
     * those encoded DataCloud attrs, to decode them first and encode them again
     * in bucket job, we could keep same encode strategy (bucket value, number
     * of bits, etc), but assigned bit position will change because DataCloud
     * encodes thousands of attrs into single encoded attrs, while here, every
     * encoded attr only has 64 bits
     *
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
            Map<String, ProfileParameters.Attribute> decAttrsMap, Map<String, String> decodeStrs) {
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
            bucket.setCategories(new ArrayList<>(Arrays.asList(valueDictArr)));
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

    /**
     * To encode numeric attrs and categorical attrs. Only serve for
     * AccountMasterStatistic job
     */
    private void postProcessProfiledAttrs(String avroDir, ProfileConfig config, ProfileParameters paras) {
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, avroDir + "/*.avro");
        Map<String, Attribute> numericAttrsMap = new HashMap<>(); // attr name -> attr
        paras.getNumericAttrs().forEach(numericAttr -> numericAttrsMap.put(numericAttr.getAttr(), numericAttr));
        Map<String, Attribute> catAttrsMap = new HashMap<>();    // attr name -> attr
        paras.getCatAttrs().forEach(catAttr -> catAttrsMap.put(catAttr.getAttr(), catAttr));
        for (GenericRecord record : records) {
            String attrName = record.get(DataCloudConstants.PROFILE_ATTR_ATTRNAME).toString();
            boolean readyForNext;
            readyForNext = AttrClassifier.isEncodeDisabledAttr(attrName, record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO),
                    paras, getAttrsToRetain(), numericAttrsMap, catAttrsMap);
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
        // Move numerical & categorical attrs to encode attrs
        String dataCloudVersion = findDCVersionToProfile(config);
        Map<String, ProfileArgument> amAttrsConfig = findAMAttrsConfig(config, dataCloudVersion);
        for (ProfileParameters.Attribute numAttr : paras.getNumericAttrs()) {
            if (amAttrsConfig.containsKey(numAttr.getAttr())) {
                paras.getAmAttrsToEnc().add(numAttr);
            } else {
                paras.getExAttrsToEnc().add(numAttr);
            }
        }
        for (ProfileParameters.Attribute catAttr : paras.getCatAttrs()) {
            if (amAttrsConfig.containsKey(catAttr.getAttr())) {
                paras.getAmAttrsToEnc().add(catAttr);
            } else {
                paras.getExAttrsToEnc().add(catAttr);
            }
        }

        // paras.getAmAttrsToEnc().addAll(paras.getNumericAttrs());
        // paras.getAmAttrsToEnc().addAll(paras.getCatAttrs());
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
        int size = result.size() + paras.getNumericAttrs().size() + paras.getCatAttrs().size() + amAttrsToEnc.size()
                + exAttrsToEnc.size();
        switch (config.getStage()) {
        case DataCloudConstants.PROFILE_STAGE_ENRICH:
            log.info(String.format(
                    "%d numeric attrs(grouped into encode attrs and retain attrs), "
                            + "%d categorical attrs(grouped into encode attrs and retain attrs), "
                            + "%d am attrs to encode, %d external attrs to encode, %d attrs to retain",
                    paras.getNumericAttrs().size(), paras.getCatAttrs().size(), amAttrsToEnc.size(),
                    exAttrsToEnc.size(), getAttrsToRetain().size()));
            break;
        case DataCloudConstants.PROFILE_STAGE_SEGMENT:
            log.info(String.format(
                    "%d numeric attrs, %d categorical attrs, "
                            + "%d am attrs to encode, %d external attrs to encode, %d attrs to retain",
                    paras.getNumericAttrs().size(), paras.getCatAttrs().size(), amAttrsToEnc.size(),
                    exAttrsToEnc.size(), getAttrsToRetain().size()));
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

    /**
     * AttrName: attr name in target source after bucketing
     * SrcAttr: original attr name in base source to profile/bucket, to serve some rename purpose
     * DecodeStrategy: original attr is encoded, to serve AccountMasterStatistics job. No need for ProfileAccount job in PA
     * EncAttr: if the attr needs to encode in bucket job, it's the encode attr name
     * LowestBit: if the attr needs to encode in bucket job, it's the lowest bit position of this attr in encode attr
     * NumBits: if the attr needs to encode in bucket job, how many bits it needs
     * BktAlgo: serialized BucketAlgorithm
     */
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

    // Schema: AttrName, SrcAttr, DecodeStrategy, EncAttr, LowestBit, NumBits,
    // BktAlgo
    // For ProfileAccount job in PA, idAttr is always LatticeAccountId
    // For AccountMasterStatistics job, rename idAttr from LatticeID in original AccountMaster to LatticeAccountId in AccountMasterStatistics
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

    // Schema: AttrName, SrcAttr, DecodeStrategy, EncAttr, LowestBit, NumBits,
    // BktAlgo
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

    // Schema: AttrName, SrcAttr, DecodeStrategy, EncAttr, LowestBit, NumBits,
    // BktAlgo
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

    // Schema: AttrName, SrcAttr, DecodeStrategy, EncAttr, LowestBit, NumBits, BktAlgo
    private Object[] profileDateAttr(ProfileParameters.Attribute dateAttr) {
        Object[] data = new Object[7];
        data[0] = dateAttr.getAttr();
        data[1] = dateAttr.getAttr();
        data[2] = null;
        data[3] = null;
        data[4] = null;
        data[5] = null;
        data[6] = dateAttr.getAlgo() == null ? null : JsonUtils.serialize(dateAttr.getAlgo());
        return data;
    }

    /**
     * Group attrs to encode. Different attrs require different number of bits.
     * Each encoded attr has 64 bits
     */
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
            String encodedAttr;
            List<ProfileParameters.Attribute> attachedAttrs;
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
        return encAttrPrefix + encodedSeq;
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

        @SuppressWarnings("unused")
        public Boolean isProfile() {
            return isProfile;
        }

        BitDecodeStrategy getDecodeStrategy() {
            return decodeStrategy;
        }

        String getBktAlgo() {
            return bktAlgo;
        }

        Boolean isNoBucket() {
            return noBucket;
        }

        Integer getNumBits() {
            return numBits;
        }
    }


    private static class AttrClassifier {

        private static boolean isEncodedAttr(ProfileArgument arg) {
            return arg.decodeStrategy != null;
        }

        private static boolean isProfileEnabled(ProfileArgument arg) {
            return arg.isProfile;
        }

        private static boolean isIdAttr(Field field, ProfileParameters paras) {
            if (field.name().equals(DataCloudConstants.LATTICE_ID)
                    || field.name().equals(DataCloudConstants.LATTICE_ACCOUNT_ID)) {
                // If profiled source is AccountMaster, it has both LatticeID
                // (long type) & LatticeAccountId (string type) with same value
                // (Reason: LatticeID is from AMSeed. LatticeAccountId is
                // generated in AMCleaner transformer to copy value from
                // LatticeID and convert to string type).
                // If profiled source is match result, it should only have
                // LatticeAccountId.
                // We always prefer LatticeAccountId as ID attr
                if (DataCloudConstants.LATTICE_ACCOUNT_ID.equals(paras.getIdAttr())) {
                    log.info("Found ID attr {} and discard extra ID {}", DataCloudConstants.LATTICE_ACCOUNT_ID,
                            field.name());
                    // extra ID attr will be discarded in isAttrToDiscard
                    return false;
                }
                if (DataCloudConstants.LATTICE_ACCOUNT_ID.equals(field.name()) && paras.getIdAttr() != null) {
                    log.info("Found ID attr {} and discard extra ID {}", DataCloudConstants.LATTICE_ACCOUNT_ID,
                            paras.getIdAttr());
                } else {
                    log.info(String.format("ID attr: %s (unencode)", field.name()));
                }
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
                log.debug(String.format("Discarded attr: %s", field.name()));
                return true;
            }
            return false;
        }

        private static boolean isAttrNoBucket(Field field, Map<String, ProfileArgument> amAttrConfig,
                List<Attribute> attrsToRetain) {
            if (!amAttrConfig.containsKey(field.name())) {
                return false;
            }
            if (Boolean.FALSE.equals(amAttrConfig.get(field.name()).isNoBucket())) {
                return false;
            }
            log.debug(String.format("Retained attr: %s (unencode)", field.name()));
            attrsToRetain.add(new ProfileParameters.Attribute(field.name(), null, null,null));
            return true;
        }

        private static boolean isPreknownAttr(Field field, Map<String, List<ProfileParameters.Attribute>> encAttrsMap,
                Map<String, ProfileParameters.Attribute> decAttrsMap, Set<String> encAttrs, ProfileConfig config,
                ProfileParameters paras, List<Attribute> attrsToRetain) {
            // Preknown attributes which are encoded (usually for Enrichment use
            // case, since profiled source is AM)
            if (encAttrsMap.containsKey(field.name())) {
                for (ProfileParameters.Attribute attr : encAttrsMap.get(field.name())) {
                    classifyPreknownAttr(attr, config.getStage(), paras.getNumericAttrs(), paras.getCatAttrs(),
                            paras.getAmAttrsToEnc(), attrsToRetain);
                }
                return true;
            }
            // Preknown attributes which are in plain format (usually for
            // Segment use case, since profiled source is match result)
            if (decAttrsMap.containsKey(field.name())) {
                classifyPreknownAttr(decAttrsMap.get(field.name()), config.getStage(), paras.getNumericAttrs(),
                        paras.getCatAttrs(), paras.getAmAttrsToEnc(), attrsToRetain);
                return true;
            }
            if (encAttrs.contains(field.name())) {
                log.debug(String.format(
                        "Ignore encoded attr: %s (No decoded attrs of it are enabled in profiling)",
                        field.name()));
                return true;
            }
            return false;
        }

        private static boolean isNumericAttr(Field field, ProfileConfig config, ProfileParameters paras) {
            Schema.Type type = AvroUtils.getType(field);
            if (NUM_TYPES.contains(type)) {
                // Skip numerical attributes that are actually Date timestamps since they are processed separately.
                if (field.getProp("logicalType") != null && field.getProp("logicalType").equals("Date")) {
                    return false;
                }
                switch (config.getStage()) {
                case DataCloudConstants.PROFILE_STAGE_SEGMENT:
                    log.debug(String.format("Interval bucketed attr %s (type %s unencode)", field.name(),
                            type.getName()));
                    break;
                case DataCloudConstants.PROFILE_STAGE_ENRICH:
                    log.debug(
                            String.format("Interval bucketed attr %s (type %s encode)", field.name(), type.getName()));
                    break;
                default:
                    throw new RuntimeException("Unrecognized stage " + config.getStage());
                }
                paras.getNumericAttrs()
                        .add(new ProfileParameters.Attribute(field.name(), null, null,
                                new IntervalBucket()));
                return true;
            }
            return false;
        }

        private static boolean isBooleanAttr(Field field, Map<String, ProfileArgument> amAttrConfig,
                ProfileParameters paras) {
            Schema.Type type = AvroUtils.getType(field);
            if (BOOL_TYPES.contains(type)
                    || FundamentalType.BOOLEAN.getName().equalsIgnoreCase(field.getProp(AVRO_PROP_KEY))) {
                log.debug(String.format("Boolean bucketed attr %s (type %s encode)", field.name(), type.getName()));
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
            Schema.Type type = AvroUtils.getType(field);
            if (CAT_TYPES.contains(type)) {
                switch (config.getStage()) {
                case DataCloudConstants.PROFILE_STAGE_SEGMENT:
                    log.debug(String.format("Categorical bucketed attr %s (type %s unencode)", field.name(),
                            type.getName()));
                    break;
                case DataCloudConstants.PROFILE_STAGE_ENRICH:
                    log.debug(String.format("Categorical bucketed attr %s (type %s encode)", field.name(),
                            type.getName()));
                    break;
                default:
                    throw new RuntimeException("Unrecognized stage " + config.getStage());
                }
                paras.getCatAttrs()
                        .add(new ProfileParameters.Attribute(field.name(), null, null,
                                new CategoricalBucket()));
                return true;
            }
            return false;
        }

        private static boolean isDateAttr(Field field, Map<String, ProfileArgument> amAttrConfig, ProfileConfig config,
                                          List<Attribute> dateAttrs) {
            // Currently, date attributes in the Account Master are not handled by the Date Attributes feature.  Skip
            // these for now.
            if (amAttrConfig.containsKey(field.name())) {
                return false;
            }

            Schema.Type type = AvroUtils.getType(field);
            // Make sure the schema type is Long which is the only supported type for dates.
            if (DATE_TYPES.contains(type)) {
                // Check that the field has Logical Type "Date" set.
                if (field.getProp("logicalType") != null && field.getProp("logicalType").equals("Date")) {
                    log.debug(String.format("Date bucketed attr %s (type %s unencode)", field.name(), type.getName()));
                    dateAttrs.add(new ProfileParameters.Attribute(
                            field.name(), null, null,
                            new DateBucket(config.getEvaluationDateAsTimestamp())));
                    return true;
                }
            }
            return false;
        }

        private static boolean isAttrToRetain(Field field, List<Attribute> attrsToRetain) {
            log.debug(String.format("Retained attr: %s (unencode)", field.name()));
            attrsToRetain.add(new ProfileParameters.Attribute(
                    field.name(), null, null, null));
            return true;
        }

        private static void classifyPreknownAttr(ProfileParameters.Attribute attr, String stage,
                List<Attribute> numericAttrs,
                List<Attribute> catAttrs, List<Attribute> amAttrsToEnc, List<Attribute> attrsToRetain) {
            switch (stage) {
            case DataCloudConstants.PROFILE_STAGE_SEGMENT:
                if (attr.getAlgo() instanceof BooleanBucket || attr.getAlgo() instanceof CategoricalBucket) {
                    log.debug(String.format("%s attr %s (encode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    amAttrsToEnc.add(attr);
                } else if (attr.getAlgo() instanceof IntervalBucket) {
                    log.debug(String.format("%s attr %s (unencode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    numericAttrs.add(attr);
                } else {
                    log.debug(String.format("%s attr %s (unencode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    attrsToRetain.add(attr);
                }
                break;
            case DataCloudConstants.PROFILE_STAGE_ENRICH:
                if (attr.getAlgo() instanceof BooleanBucket || attr.getAlgo() instanceof CategoricalBucket) {
                    log.debug(String.format("%s attr %s (encode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    amAttrsToEnc.add(attr);
                } else if (attr.getAlgo() instanceof IntervalBucket) {
                    log.debug(String.format("%s attr %s (encode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    numericAttrs.add(attr);
                } else {
                    log.debug(String.format("%s attr %s (unencode)", attr.getAlgo().getClass().getSimpleName(),
                            attr.getAttr()));
                    attrsToRetain.add(attr);
                }
                break;
            default:
                throw new RuntimeException("Unrecognized stage " + stage);
            }
        }

        private static boolean isEncodeDisabledAttr(String attrName, Object bucketAlgo, ProfileParameters paras,
                                              List<Attribute> attrsToRetain, Map<String, Attribute> numericAttrsMap,
                                              Map<String, Attribute> catAttrsMap) {
            Set<String> catAttrsNotEnc = paras.getCatAttrsNotEnc() != null
                    ? new HashSet<>(Arrays.asList(paras.getCatAttrsNotEnc()))
                    : new HashSet<>();
            if (numericAttrsMap.containsKey(attrName)) {
                IntervalBucket algo = deserializeIntervalBucket(bucketAlgo);
                if (algo != null && CollectionUtils.isNotEmpty(algo.getBoundaries())) {
                    return false;
                }
                numericAttrsMap.get(attrName).setAlgo(null);
                attrsToRetain.add(numericAttrsMap.get(attrName));
                paras.getNumericAttrs().remove(numericAttrsMap.get(attrName));
                log.warn(String.format(
                        "Attribute %s is moved from encode numeric group to retained group due to all the values are null: %s",
                        attrName, JsonUtils.serialize(numericAttrsMap.get(attrName))));
                return true;
            }
            if (catAttrsMap.containsKey(attrName)) {
                if (bucketAlgo != null && !catAttrsNotEnc.contains(attrName)) {
                    return false;
                }
                catAttrsMap.get(attrName).setAlgo(null);
                attrsToRetain.add(catAttrsMap.get(attrName));
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

        /**
         * @param bucketAlgo
         *            (String / org.apache.avro.util.Utf8)
         * @return
         */
        private static IntervalBucket deserializeIntervalBucket(Object bucketAlgo) {
            if (bucketAlgo == null) {
                return null;
            }
            try {
                return JsonUtils.deserialize(bucketAlgo.toString(), IntervalBucket.class);
            } catch (Exception e) {
                return null;
            }
        }
    }
}
