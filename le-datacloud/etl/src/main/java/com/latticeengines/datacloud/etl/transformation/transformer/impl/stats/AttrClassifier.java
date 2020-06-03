package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.LATTICE_ACCOUNT_ID;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.LATTICE_ID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.util.BitCodeBookUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategorizedIntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DateBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.stats.ProfileConfig;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;

public class AttrClassifier {

    private static final Logger log = LoggerFactory.getLogger(AttrClassifier.class);

    private static final Set<String> NUM_CLASSES = Sets.newHashSet("integer", "long", "float", "double");
    private static final Set<String> BOOL_CLASSES = Sets.newHashSet("boolean");
    private static final Set<String> CAT_CLASSES = Sets.newHashSet("string");
    private static final Set<String> DATE_CLASSES = Sets.newHashSet("long");

    // List of Attributes classified as date attributes.
    private final List<ProfileParameters.Attribute> dateAttrs = new ArrayList<>();

    // List of Attributes to retain which will be defined in the SourceProfiler class itself, rather than through
    // the Profile data flow.
    private final List<ProfileParameters.Attribute> attrsToRetain = new ArrayList<>();

    // List of Attributes with declared profile, excluding those not in the input or to be encoded
    private final List<ProfileParameters.Attribute> declaredAttrs = new ArrayList<>();

    // encoded attr -> [decoded attrs] (Enabled in profiling)
    private final Map<String, List<ProfileParameters.Attribute>> encAttrsMap = new HashMap<>();

    // all encoded attrs (enabled/disabled in profiling)
    private final Set<String> encAttrs = new HashSet<>();

    // decoded attr name -> decoded attr object
    private final Map<String, ProfileParameters.Attribute> decAttrsMap = new HashMap<>();

    // decoded attr -> decode strategy str
    private final Map<String, String> decodeStrs = new HashMap<>();

    private final ProfileParameters params;
    private final ProfileJobConfig jobConfig;
    private final String stage;
    private final long evaluationTime;
    private final Map<String, ProfileArgument> amAttrConfig;
    private final Map<String, ProfileParameters.Attribute> declaredAttrConfig;
    private final boolean detectCategorical;
    private final String encAttrPrefix;
    private final int encodeBits;
    private final int maxAttrs;
    private final boolean useSpark;
    private String idAttr;

    public AttrClassifier(ProfileJobConfig jobConfig, Map<String, ProfileArgument> amAttrConfig,
                          Map<String, ProfileParameters.Attribute> declaredAttrConfig,
                          int encodeBits, int maxAttrs) {
        this.useSpark = true;
        this.params = null;
        this.jobConfig = jobConfig;
        this.detectCategorical = jobConfig.isAutoDetectCategorical();

        this.stage = jobConfig.getStage();
        if (jobConfig.getEvaluationDateAsTimestamp() == -1) {
            // Make sure the evaluation date is set to something.
            // If it isn't, then set it to the beginning of today at UTC.
            long evalDate = ProfileUtils.getBeginningOfTodayAsEvaluationDate();
            jobConfig.setEvaluationDateAsTimestamp(evalDate);
            log.warn("Evaluation Date not set before SourceProfiler, setting to " + evalDate);
        }
        this.evaluationTime = jobConfig.getEvaluationDateAsTimestamp();
        this.amAttrConfig = amAttrConfig;
        this.declaredAttrConfig = declaredAttrConfig;
        this.encAttrPrefix = jobConfig.getEncAttrPrefix();
        this.idAttr = jobConfig.getIdAttr();
        this.encodeBits = encodeBits;
        this.maxAttrs = maxAttrs;
    }

    public AttrClassifier(ProfileParameters params, ProfileConfig config, Map<String, ProfileArgument> amAttrConfig,
            int encodeBits, int maxAttrs) {
        this.useSpark = false;
        this.detectCategorical = true;
        this.params = params;
        this.jobConfig = null;

        this.stage = config.getStage();
        if (config.getEvaluationDateAsTimestamp() == -1) {
            // Make sure the evaluation date is set to something.
            // If it isn't, then set it to the beginning of today at UTC.
            long evalDate = ProfileUtils.getBeginningOfTodayAsEvaluationDate();
            config.setEvaluationDateAsTimestamp(evalDate);
            log.warn("Evaluation Date not set before SourceProfiler, setting to " + evalDate);
        }
        this.evaluationTime = config.getEvaluationDateAsTimestamp();
        this.amAttrConfig = amAttrConfig;
        this.encAttrPrefix = config.getEncAttrPrefix();
        this.declaredAttrConfig = new HashMap<>();
        this.idAttr = params.getIdAttr();
        this.encodeBits = encodeBits;
        this.maxAttrs = maxAttrs;
    }

    private void scanIdAttr(List<ColumnMetadata> cms) {
        String idAttr = null;
        for (ColumnMetadata cm: cms) {
            String attrName = cm.getAttrName();
            if (LATTICE_ACCOUNT_ID.equals(attrName) || LATTICE_ID.equals(attrName)) {
                // If profiled source is AccountMaster, it has both LatticeID
                // (long type) & LatticeAccountId (string type) with same value
                // (Reason: LatticeID is from AMSeed. LatticeAccountId is
                // generated in AMCleaner transformer to copy value from
                // LatticeID and convert to string type).
                // If profiled source is match result, it should only have
                // LatticeAccountId.
                // We always prefer LatticeAccountId as ID attr
                if (idAttr == null || LATTICE_ID.equals(idAttr)) {
                    idAttr = attrName;
                }
            }
        }
        log.info(String.format("ID attr: %s (unencode)", idAttr));
        this.idAttr = idAttr;
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
    public void classifyAttrs(List<ColumnMetadata> cms) {
        scanIdAttr(cms);
        parseEncodedAttrsConfig();
        // Parse flat attrs in the profiled source
        for (ColumnMetadata cm : cms) {
            boolean readyForNext;
            readyForNext = isIdAttr(cm);
            if (!readyForNext) {
                readyForNext = isDeclaredAttr(cm);
            }
            if (!readyForNext) {
                readyForNext = isAttrToDiscard(cm);
            }
            if (!readyForNext) {
                readyForNext = isAttrNoBucket(cm);
            }
            if (!readyForNext) {
                readyForNext = isPreknownAttr(cm);
            }
            if (!readyForNext) {
                readyForNext = isDateAttr(cm);
            }
            if (!readyForNext) {
                readyForNext = isNumericAttr(cm);
            }
            if (!readyForNext) {
                readyForNext = isBooleanAttr(cm);
            }
            if (!readyForNext) {
                readyForNext = isCategoricalAttr(cm);
            }
            if (!readyForNext) {
                addAttrToRetain(cm);
            }
        }
    }

    private boolean isIdAttr(ColumnMetadata column) {
        return column.getAttrName().equals(idAttr);
    }

    private boolean isDeclaredAttr(ColumnMetadata column) {
        boolean declared = false;
        if (declaredAttrConfig.containsKey(column.getAttrName())) {
            ProfileParameters.Attribute declaredAttr = declaredAttrConfig.get(column.getAttrName());
            if (declaredAttr.getAlgo() == null) {
                attrsToRetain.add(declaredAttr);
            } if (declaredAttr.getAlgo() instanceof CategoricalBucket) {
                declaredAttrs.add(declaredAttr);
            }
            declared = true;
        }
        return declared;
    }

    private boolean isAttrToDiscard(ColumnMetadata column) {
        boolean discard = false;
        if (amAttrConfig.containsKey(column.getAttrName()) &&
                Boolean.FALSE.equals(amAttrConfig.get(column.getAttrName()).isProfile)) {
            log.debug(String.format("Discarded attr: %s", column.getAttrName()));
            discard = true;
        }
        return discard;
    }

    private boolean isAttrNoBucket(ColumnMetadata column) {
        boolean noBucket;
        if (!amAttrConfig.containsKey(column.getAttrName())) {
            // always bucket non AM attrs
            noBucket = false;
        } else {
            noBucket = !Boolean.FALSE.equals(amAttrConfig.get(column.getAttrName()).isNoBucket());
        }
        if (noBucket) {
            log.debug(String.format("Retained attr: %s (unencode)", column.getAttrName()));
            attrsToRetain.add(new ProfileParameters.Attribute(column.getAttrName(), null, null, null));
        }
        return noBucket;
    }

    private boolean isPreknownAttr(ColumnMetadata column) {
        boolean known = false;
        if (encAttrsMap.containsKey(column.getAttrName())) {
            // Preknown attributes which are encoded (usually for Enrichment use
            // case, since profiled source is AM)
            for (ProfileParameters.Attribute attr : encAttrsMap.get(column.getAttrName())) {
                classifyPreknownAttr(attr, stage, getNumericAttrs(), getAmAttrsToEnc(), attrsToRetain);
            }
            known = true;
        } else if (decAttrsMap.containsKey(column.getAttrName())) {
            // Preknown attributes which are in plain format (usually for
            // Segment use case, since profiled source is match result)
            classifyPreknownAttr(decAttrsMap.get(column.getAttrName()), stage, getNumericAttrs(),
                    getAmAttrsToEnc(), attrsToRetain);
            known = true;
        } else if (encAttrs.contains(column.getAttrName())) {
            log.debug(String.format("Ignore encoded attr: %s (No decoded attrs of it are enabled in profiling)",
                    column.getAttrName()));
            known = true;
        }
        return known;
    }

    private boolean isNumericAttr(ColumnMetadata column) {
        boolean isNumeric = false;
        if (NUM_CLASSES.contains(column.getJavaClass().toLowerCase())) {
            if (!LogicalDataType.Date.equals(column.getLogicalDataType())) {
                // Skip numerical attributes that are actually Date timestamps since they are processed separately.
                isNumeric = true;
            }
        }
        if (isNumeric) {
            getNumericAttrs()
                    .add(new ProfileParameters.Attribute(column.getAttrName(), null, null,
                            new IntervalBucket()));
        }
        return isNumeric;
    }

    private boolean isBooleanAttr(ColumnMetadata column) {
        if (BOOL_CLASSES.contains(column.getJavaClass().toLowerCase())
                || FundamentalType.BOOLEAN.equals(column.getFundamentalType())) {
            log.debug(String.format("Boolean bucketed attr %s (type %s encode)", column.getAttrName(),
                    column.getJavaClass()));
            BucketAlgorithm algo = new BooleanBucket();
            if (amAttrConfig.containsKey(column.getAttrName())) {
                getAmAttrsToEnc()
                        .add(new ProfileParameters.Attribute(column.getAttrName(), 2, null, algo));
            } else {
                getExAttrsToEnc()
                        .add(new ProfileParameters.Attribute(column.getAttrName(), 2, null, algo));
            }
            return true;
        }
        return false;
    }

    private boolean isCategoricalAttr(ColumnMetadata column) {
        if (detectCategorical && CAT_CLASSES.contains(column.getJavaClass().toLowerCase())) {
            getCatAttrs()
                    .add(new ProfileParameters.Attribute(column.getAttrName(), null, null,
                            new CategoricalBucket()));
            return true;
        }
        return false;
    }

    private boolean isDateAttr(ColumnMetadata column) {
        // Currently, date attributes in the Account Master are not handled by the Date Attributes feature.  Skip
        // these for now.
        if (amAttrConfig.containsKey(column.getAttrName())) {
            return false;
        }

        // Make sure the schema type is Long which is the only supported type for dates.
        if (DATE_CLASSES.contains(column.getJavaClass().toLowerCase())) {
            // Check that the field has Logical Type "Date" set.
            if (LogicalDataType.Date.equals(column.getLogicalDataType())) {
                log.debug(String.format("Date bucketed attr %s (type %s unencode)", column.getAttrName(),
                        column.getJavaClass()));
                dateAttrs.add(new ProfileParameters.Attribute(
                        column.getAttrName(), null, null,
                        new DateBucket(evaluationTime)));
                return true;
            }
        }
        return false;
    }

    private void addAttrToRetain(ColumnMetadata column) {
        log.debug(String.format("Retained attr: %s (unencode)", column.getAttrName()));
        attrsToRetain.add(new ProfileParameters.Attribute(
                column.getAttrName(), null, null, null));
    }

    /**
     * Some DataCloud attrs are encoded already. We need to put DecodeStrategy
     * in profile result so that bucket job will know how to decode them. For
     * those encoded DataCloud attrs, to decode them first and encode them again
     * in bucket job, we could keep same encode strategy (bucket value, number
     * of bits, etc), but assigned bit position will change because DataCloud
     * encodes thousands of attrs into single encoded attrs, while here, every
     * encoded attr only has 64 bits
     */
    public void parseEncodedAttrsConfig() {
        for (Map.Entry<String, ProfileArgument> amAttrConfig : this.amAttrConfig.entrySet()) {
            if (!ProfileUtils.isEncodedAttr(amAttrConfig.getValue())) {
                continue;
            }
            BitDecodeStrategy decodeStrategy = amAttrConfig.getValue().getDecodeStrategy();
            String decodeStrategyStr = JsonUtils.serialize(decodeStrategy);
            String encAttr = decodeStrategy.getEncodedColumn();
            encAttrs.add(encAttr);
            if (!ProfileUtils.isProfileEnabled(amAttrConfig.getValue())) {
                continue;
            }
            if (!encAttrsMap.containsKey(encAttr)) {
                encAttrsMap.put(encAttr, new ArrayList<>());
            }
            validateEncodedSrcAttrArg(amAttrConfig.getKey(), amAttrConfig.getValue());
            ProfileArgument profileArg = amAttrConfig.getValue();
            BucketAlgorithm bktAlgo = parseBucketAlgo(profileArg.getBktAlgo(), decodeStrategy.getValueDict());
            Integer numBits = amAttrConfig.getValue().getNumBits() != null ? amAttrConfig.getValue().getNumBits()
                    : decideBitNumFromBucketAlgo(bktAlgo);
            ProfileParameters.Attribute attr = new ProfileParameters.Attribute(amAttrConfig.getKey(), numBits,
                    decodeStrategyStr, bktAlgo);
            encAttrsMap.get(encAttr).add(attr);
            decAttrsMap.put(attr.getAttr(), attr);
            decodeStrs.put(amAttrConfig.getKey(), decodeStrategyStr);
        }

        // Build BitCodeBook
        BitCodeBookUtils.constructCodeBookMap(getCodeBookMap(), getCodeBookLookup(), decodeStrs);
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

    private void classifyPreknownAttr(ProfileParameters.Attribute attr, String stage,
                                             List<ProfileParameters.Attribute> numericAttrs,
                                             List<ProfileParameters.Attribute> amAttrsToEnc,
                                             List<ProfileParameters.Attribute> attrsToRetain) {
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
                    if (attr.getEncodeBitUnit() == null) {
                        log.info("Adding null bit unit to attrs to enc " + JsonUtils.serialize(attr));
                    }
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

    public Object[][] parseResult() {
        List<Object[]> result = new ArrayList<>();
        if (idAttr != null) {
            result.add(ProfileUtils.profileIdAttr(idAttr));
        }
        for (ProfileParameters.Attribute dateAttr : dateAttrs) {
            result.add(ProfileUtils.profileDateAttr(dateAttr));
        }
        for (ProfileParameters.Attribute attr : attrsToRetain) {
            result.add(ProfileUtils.profileAttrToRetain(attr));
        }
        for (ProfileParameters.Attribute attr : declaredAttrs) {
            result.add(ProfileUtils.profileDeclared(attr));
        }
        Map<String, List<ProfileParameters.Attribute>> amAttrsToEnc = groupAttrsToEnc(getAmAttrsToEnc(),
                DataCloudConstants.EAttr);
        Map<String, List<ProfileParameters.Attribute>> exAttrsToEnc = groupAttrsToEnc(getExAttrsToEnc(),
                StringUtils.isBlank(encAttrPrefix) ? DataCloudConstants.CEAttr : encAttrPrefix);

        reviewFinalAttrGroup(result);
        for (Map.Entry<String, List<ProfileParameters.Attribute>> entry : amAttrsToEnc.entrySet()) {
            int lowestBit = 0;
            for (ProfileParameters.Attribute attr : entry.getValue()) {
                result.add(ProfileUtils.profileAttrToEnc(attr, entry.getKey(), lowestBit));
                lowestBit += attr.getEncodeBitUnit();
            }
        }
        for (Map.Entry<String, List<ProfileParameters.Attribute>> entry : exAttrsToEnc.entrySet()) {
            int lowestBit = 0;
            for (ProfileParameters.Attribute attr : entry.getValue()) {
                result.add(ProfileUtils.profileAttrToEnc(attr, entry.getKey(), lowestBit));
                lowestBit += attr.getEncodeBitUnit();
            }
        }
        return result.toArray(new Object[0][]);
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
        for (ProfileParameters.Attribute attr: attrs) {
            if (attr.getEncodeBitUnit() == null) {
                log.info(JsonUtils.serialize(attr));
            }
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

    private void reviewFinalAttrGroup(List<Object[]> result) {
        int size = result.size() + getNumericAttrs().size() + getCatAttrs().size() + getAmAttrsToEnc().size()
                + getExAttrsToEnc().size();
        switch (stage) {
            case DataCloudConstants.PROFILE_STAGE_ENRICH:
                log.info(String.format(
                        "%d numeric attrs(grouped into encode attrs and retain attrs), "
                                + "%d categorical attrs(grouped into encode attrs and retain attrs), "
                                + "%d am attrs to encode, %d external attrs to encode, %d attrs to retain",
                        getNumericAttrs().size(), getCatAttrs().size(), getAmAttrsToEnc().size(),
                        getExAttrsToEnc().size(), attrsToRetain.size()));
                break;
            case DataCloudConstants.PROFILE_STAGE_SEGMENT:
                log.info(String.format(
                        "%d numeric attrs, %d categorical attrs, "
                                + "%d am attrs to encode, %d external attrs to encode, %d attrs to retain",
                        getNumericAttrs().size(), getCatAttrs().size(), getAmAttrsToEnc().size(),
                        getExAttrsToEnc().size(), attrsToRetain.size()));
                break;
            default:
                throw new RuntimeException("Unrecognized stage " + stage);
        }
        if (DataCloudConstants.PROFILE_STAGE_SEGMENT.equals(stage) && size > maxAttrs) {
            log.warn(String.format("Attr num after bucket and encode is %d, exceeding expected maximum limit %d", size,
                    maxAttrs));
        } else {
            log.info(String.format("Attr num after bucket and encode: %d", size));
        }
    }

    private String createEncodeAttrName(String encAttrPrefix, int encodedSeq) {
        return encAttrPrefix + encodedSeq;
    }

    public List<ProfileParameters.Attribute> getAttrsToRetain() {
        return attrsToRetain;
    }

    private List<ProfileParameters.Attribute> getNumericAttrs() {
        return useSpark ? jobConfig.getNumericAttrs() : params.getNumericAttrs();
    }

    private List<ProfileParameters.Attribute> getCatAttrs() {
        return useSpark ? jobConfig.getCatAttrs() : params.getCatAttrs();
    }

    private List<ProfileParameters.Attribute> getAmAttrsToEnc() {
        return useSpark ? jobConfig.getAmAttrsToEnc() : params.getAmAttrsToEnc();
    }

    private List<ProfileParameters.Attribute> getExAttrsToEnc() {
        return useSpark ? jobConfig.getExAttrsToEnc() : params.getExAttrsToEnc();
    }

    private Map<String, BitCodeBook> getCodeBookMap() {
        return useSpark ? jobConfig.getCodeBookMap() : params.getCodeBookMap();
    }

    private Map<String, String> getCodeBookLookup() {
        return useSpark ? jobConfig.getCodeBookLookup() : params.getCodeBookLookup();
    }

}
