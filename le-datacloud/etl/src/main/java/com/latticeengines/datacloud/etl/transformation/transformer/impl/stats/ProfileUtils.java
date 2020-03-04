package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;

public final class ProfileUtils {

    private static final Logger log = LoggerFactory.getLogger(ProfileUtils.class);

    protected ProfileUtils() {
        throw new UnsupportedOperationException();
    }

    static long getBeginningOfTodayAsEvaluationDate() {
        return LocalDate.now().atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();
    }

    /**
     * AttrName: attr name in target source after bucketing SrcAttr: original attr
     * name in base source to profile/bucket, to serve some rename purpose
     * DecodeStrategy: original attr is encoded, to serve AccountMasterStatistics
     * job. No need for ProfileAccount job in PA EncAttr: if the attr needs to
     * encode in bucket job, it's the encode attr name LowestBit: if the attr needs
     * to encode in bucket job, it's the lowest bit position of this attr in encode
     * attr NumBits: if the attr needs to encode in bucket job, how many bits it
     * needs BktAlgo: serialized BucketAlgorithm
     */
    static List<Pair<String, Class<?>>> getProfileSchema() {
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

    static boolean isEncodedAttr(ProfileArgument arg) {
        return arg.getDecodeStrategy() != null;
    }

    static boolean isProfileEnabled(ProfileArgument arg) {
        return arg.isProfile();
    }

    static boolean isEncodeDisabledAttr(String attrName, Object bucketAlgo, ProfileJobConfig paras,
                                        List<ProfileParameters.Attribute> attrsToRetain, Map<String, ProfileParameters.Attribute> numericAttrsMap,
                                        Map<String, ProfileParameters.Attribute> catAttrsMap) {
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

    static boolean isEncodeDisabledAttr(String attrName, Object bucketAlgo, ProfileParameters paras,
            List<ProfileParameters.Attribute> attrsToRetain, Map<String, ProfileParameters.Attribute> numericAttrsMap,
            Map<String, ProfileParameters.Attribute> catAttrsMap) {
        Set<String> catAttrsNotEnc = paras.getCatAttrsNotEnc() != null
                ? new HashSet<>(Arrays.asList(paras.getCatAttrsNotEnc()))
                : new HashSet<>();
        if (numericAttrsMap.containsKey(attrName)) {
            IntervalBucket algo = deserializeIntervalBucket(bucketAlgo);
            if (algo != null && CollectionUtils.isNotEmpty(algo.getBoundaries())) {
                return false;
            } else {
//                log.warn(String.format(
//                        "Attribute %s is moved from encode numeric group to retained group due to all the values are null: %s",
//                        attrName, JsonUtils.serialize(numericAttrsMap.get(attrName))));
                numericAttrsMap.get(attrName).setAlgo(null);
                attrsToRetain.add(numericAttrsMap.get(attrName));
                paras.getNumericAttrs().remove(numericAttrsMap.get(attrName));
                return true;
            }
        }
        if (catAttrsMap.containsKey(attrName)) {
            if (bucketAlgo != null && !catAttrsNotEnc.contains(attrName)) {
                return false;
            } else {
//                log.warn(String.format(
//                        "Attribute %s is moved from encode categorical group to retained group due to there is no buckets or it is a dimensional attribute for stats calculation: (%s) ",
//                        attrName, JsonUtils.serialize(numericAttrsMap.get(attrName))));
                catAttrsMap.get(attrName).setAlgo(null);
                attrsToRetain.add(catAttrsMap.get(attrName));
                paras.getCatAttrs().remove(catAttrsMap.get(attrName));
                return true;
            }
        }
        throw new RuntimeException(String.format(
                "Attribute %s in profiled result has null bucket algorithm and does not belong to either numerical attribute or categorical attribute",
                attrName));
    }

    static boolean isIntervalBucketAttr(String attrName, Object bucketAlgo,
            Map<String, ProfileParameters.Attribute> numericAttrsMap) {
        try {
            IntervalBucket algo = JsonUtils.deserialize(bucketAlgo.toString(), IntervalBucket.class);
            if (!numericAttrsMap.containsKey(attrName)) {
                throw new RuntimeException(
                        String.format("Fail to find attribute %s with IntervalBucket %s in numeric attribute list",
                                attrName, bucketAlgo.toString()));
            }
            numericAttrsMap.get(attrName).setAlgo(algo);
            // reason for boundary+2: 1 for catNum = boundNum + 1; 1 for null
            Integer numBits = Math.max((int) Math.ceil(Math.log((algo.getBoundaries().size() + 2)) / Math.log(2)), 1);
            numericAttrsMap.get(attrName).setEncodeBitUnit(numBits);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    static boolean isDiscreteBucketAttr(String attrName, Object bucketAlgo,
            Map<String, ProfileParameters.Attribute> numericAttrsMap) {
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

    static boolean isCategoricalBucketAttr(String attrName, Object bucketAlgo,
            Map<String, ProfileParameters.Attribute> catAttrsMap) {
        try {
            CategoricalBucket algo = JsonUtils.deserialize(bucketAlgo.toString(), CategoricalBucket.class);
            if (!catAttrsMap.containsKey(attrName)) {
                throw new RuntimeException(String.format(
                        "Fail to find attribute %s with CategoricalBucket %s in categorical attribute list", attrName,
                        bucketAlgo.toString()));
            }
            catAttrsMap.get(attrName).setAlgo(algo);
            Integer numBits = Math.max((int) Math.ceil(Math.log((algo.getCategories().size() + 1)) / Math.log(2)), 1);
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

    // Schema: AttrName, SrcAttr, DecodeStrategy, EncAttr, LowestBit, NumBits,
    // BktAlgo
    // For ProfileAccount job in PA, idAttr is always LatticeAccountId
    // For AccountMasterStatistics job, rename idAttr from LatticeID in original
    // AccountMaster to LatticeAccountId in AccountMasterStatistics
    static Object[] profileIdAttr(String idAttr) {
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
    static Object[] profileAttrToRetain(ProfileParameters.Attribute attr) {
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
    static Object[] profileAttrToEnc(ProfileParameters.Attribute attr, String encodedAttr, int lowestBit) {
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

    // Schema: AttrName, SrcAttr, DecodeStrategy, EncAttr, LowestBit, NumBits,
    // BktAlgo
    static Object[] profileDateAttr(ProfileParameters.Attribute dateAttr) {
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

}
