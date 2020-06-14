package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.stats.SourceProfiler.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.transformation.stats.Profile;
import com.latticeengines.datacloud.etl.transformation.TransformerUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractDataflowTransformer;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters.Attribute;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.statistics.ProfileArgument;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.stats.ProfileConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.util.MetadataConverter;


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

    private AttrClassifier classifier;

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
        classifyAttrs(step.getBaseSources()[0], step.getBaseVersions().get(0), config, paras);
    }

    /*
     * After dataflow executed, profile result from dataflow and pre-known
     * profile result need to combine together
     */
    @Override
    protected void postDataFlowProcessing(TransformStep step, String workflowDir, ProfileParameters paras,
            ProfileConfig config) {
        // DataCloudConstants.PROFILE_STAGE_ENRICH: AccountMasterStatistics
        // DataCloudConstants.PROFILE_STAGE_SEGMENT: ProfileAccount
        if (DataCloudConstants.PROFILE_STAGE_ENRICH.equals(config.getStage())) {
            postProcessProfiledAttrs(workflowDir, config, paras);
        }
        Object[][] data = classifier.parseResult();
        List<Pair<String, Class<?>>> columns = ProfileUtils.getProfileSchema();
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
        List<ColumnMetadata> cms = schema.getFields().stream() //
                .map(field -> MetadataConverter.getAttribute(field).getColumnMetadata()).collect(Collectors.toList());

        log.info("Classifying attributes...");
        try {
            classifier = new AttrClassifier(paras, config, amAttrsConfig, encodeBits, maxAttrs);
            classifier.classifyAttrs(cms);
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
            readyForNext = ProfileUtils.isEncodeDisabledAttr(attrName, record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO),
                    paras, classifier.getAttrsToRetain(), numericAttrsMap, catAttrsMap);
            if (!readyForNext) {
                readyForNext = ProfileUtils.isIntervalBucketAttr(attrName,
                        record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO), numericAttrsMap);
            }
            if (!readyForNext) {
                readyForNext = ProfileUtils.isDiscreteBucketAttr(attrName,
                        record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO), numericAttrsMap);
            }
            if (!readyForNext) {
                readyForNext = ProfileUtils.isCategoricalBucketAttr(attrName,
                        record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO), catAttrsMap);
            }
            if (!readyForNext) {
                throw new UnsupportedOperationException(String.format("Unknown bucket algorithm for attribute %s: %s",
                        attrName, record.get(DataCloudConstants.PROFILE_ATTR_BKTALGO).toString()));
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

    private void uploadAvro(String targetDir, List<Pair<String, Class<?>>> schema, Object[][] data) {
        try {
            AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, targetDir, "Profile.avro");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
