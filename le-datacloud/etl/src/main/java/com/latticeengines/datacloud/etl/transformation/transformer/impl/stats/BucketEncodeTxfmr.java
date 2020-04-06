package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.stats.BucketEncodeTxfmr.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKET_TXMFR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroParquetUtils;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.BitCodeBookUtils;
import com.latticeengines.datacloud.dataflow.utils.BucketEncodeUtils;
import com.latticeengines.datacloud.etl.transformation.TransformerUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.metadata.BucketedAttribute;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.stats.BucketEncodeConfig;
import com.latticeengines.spark.exposed.job.stats.BucketEncodeJob;

@Component(TRANSFORMER_NAME)
public class BucketEncodeTxfmr extends ConfigurableSparkJobTxfmr<BucketEncodeConfig> {

    private static final Logger log = LoggerFactory.getLogger(ProfileTxfmr.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_BUCKET_TXMFR;

    public static final String AM_PROFILE = "AMProfile";

    private int dataIdx = 0;

    @Inject
    private SourceAttributeEntityMgr srcAttrEntityMgr;

    @Inject
    private DataCloudVersionService dataCloudVersionService;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<BucketEncodeJob> getSparkJobClz() {
        return BucketEncodeJob.class;
    }

    @Override
    protected Class<BucketEncodeConfig> getJobConfigClz() {
        return BucketEncodeConfig.class;
    }

    @Override
    protected void preSparkJobProcessing(TransformStep step, String workflowDir, BucketEncodeConfig jobConfig) {
        Source profileSource = step.getBaseSources()[1];
        String profileVersion = step.getBaseVersions().get(1);

        if (!isProfileSource(profileSource, profileVersion)) {
            profileSource = step.getBaseSources()[0];
            profileVersion = step.getBaseVersions().get(0);
            if (!isProfileSource(profileSource, profileVersion)) {
                throw new RuntimeException("Neither base source has the profile schema");
            } else {
                log.info("Resolved the first base source as profile.");
                List<DataUnit> inputs = new ArrayList<>();
                inputs.add(jobConfig.getInput().get(1));
                inputs.add(jobConfig.getInput().get(0));
                jobConfig.setInput(inputs);
                dataIdx = 1;
            }
        } else {
            log.info("Resolved the second base source as profile.");
        }
        String profileAvroGlob = PathUtils.toAvroGlob(((HdfsDataUnit) jobConfig.getInput().get(1)).getPath());
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, profileAvroGlob);
        jobConfig.setEncAttrs(BucketEncodeUtils.encodedAttrs(records));
        jobConfig.setRetainAttrs(BucketEncodeUtils.retainFields(records));
        jobConfig.setRenameFields(BucketEncodeUtils.renameFields(records));

        if (DataCloudConstants.PROFILE_STAGE_ENRICH.equals(jobConfig.getStage())) {
            String dataCloudVersion = findDCVersionToProfile(jobConfig);
            Map<String, ProfileArgument> amAttrsConfig = findAMAttrsConfig(jobConfig, dataCloudVersion);
            // decoded attr -> decode strategy str
            Map<String, String> decodeStrs = new HashMap<>();
            for (Map.Entry<String, ProfileArgument> amAttrConfig : amAttrsConfig.entrySet()) {
                if (!ProfileUtils.isEncodedAttr(amAttrConfig.getValue())) {
                    continue;
                }
                BitDecodeStrategy decodeStrategy = amAttrConfig.getValue().getDecodeStrategy();
                String decodeStrategyStr = JsonUtils.serialize(decodeStrategy);
                decodeStrs.put(amAttrConfig.getKey(), decodeStrategyStr);
            }
            Map<String, BitCodeBook> codeBookMap = new HashMap<>();
            Map<String, String> codeBookLookup = new HashMap<>();
            // Build BitCodeBook
            BitCodeBookUtils.constructCodeBookMap(codeBookMap, codeBookLookup, decodeStrs);
            jobConfig.setCodeBookMap(codeBookMap);
            jobConfig.setCodeBookLookup(codeBookLookup);
        }
    }

    private boolean isProfileSource(Source source, String version) {
        String avroPath = TransformerUtils.avroPath(source, version, hdfsPathBuilder);
        Iterator<GenericRecord> records = AvroUtils.iterateAvroFiles(yarnConfiguration, avroPath);
        if (records.hasNext()) {
            GenericRecord record = records.next();
            return BucketEncodeUtils.isProfile(record);
        }
        return false;
    }

    @Override
    protected Schema getTargetSchema(HdfsDataUnit result, BucketEncodeConfig jobConfig, //
                                     TransformerConfig configuration, List<Schema> baseSchemas) {

        Schema baseSchema = baseSchemas.get(dataIdx);
        Map<String, Schema.Field> inputFields = new HashMap<>();
        baseSchema.getFields().forEach(field -> inputFields.putIfAbsent(field.name(), field));
        Schema resultSchema = AvroParquetUtils.parseAvroSchema(yarnConfiguration, result.getPath());
        Schema parsed = AvroUtils.overwriteFields(resultSchema, inputFields);
        Map<String, List<BucketedAttribute>> bktAttrMap = bktAttrMap(jobConfig.getEncAttrs());
        ObjectMapper om = new ObjectMapper();
        ObjectNode objectNode;
        try {
            objectNode = om.readValue(parsed.toString(), ObjectNode.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse avro schema of cascading result table.", e);
        }
        ArrayNode fields = (ArrayNode) objectNode.get("fields");
        for (JsonNode jNode : fields) {
            ObjectNode field = (ObjectNode) jNode;
            String fieldName = field.get("name").asText();
            if (bktAttrMap.containsKey(fieldName)) {
                field.set("bucketed_attrs", om.valueToTree(bktAttrMap.get(fieldName)));
            }
        }
        objectNode.set("fields", fields);
        Schema.Parser parser = new Schema.Parser();
        try {
            return parser.parse(om.writeValueAsString(objectNode));
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse modified schema.", e);
        }
    }

    /**
     * For ProfileAccount job in QA, look for metadata of latest approved datacloud version
     * For AccountMasterStatistics job, look for metadata of the datacloud version which is current in build (next datacloud version)
     */
    private String findDCVersionToProfile(BucketEncodeConfig config) {
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
    private Map<String, ProfileArgument> findAMAttrsConfig(BucketEncodeConfig config, String dataCloudVersion) {
        List<SourceAttribute> srcAttrs;
        if (DataCloudConstants.PROFILE_STAGE_SEGMENT.equals(config.getStage())) {
            srcAttrs = srcAttrEntityMgr.getAttributes(AM_PROFILE, config.getStage(),
                    "SourceProfiler", dataCloudVersion, true);
        } else {
            srcAttrs = srcAttrEntityMgr.getAttributes(AM_PROFILE, config.getStage(),
                    "SourceProfiler", dataCloudVersion, false);
        }
        if (CollectionUtils.isEmpty(srcAttrs)) {
            throw new RuntimeException("Fail to find configuration for profiling in SourceAttribute table");
        }
        Map<String, ProfileArgument> amAttrsConfig = new HashMap<>();
        srcAttrs.forEach(attr -> amAttrsConfig.put(attr.getAttribute(),
                JsonUtils.deserialize(attr.getArguments(), ProfileArgument.class)));
        return amAttrsConfig;
    }

    private Map<String, List<BucketedAttribute>> bktAttrMap(List<DCEncodedAttr> encAttrs) {
        Map<String, List<BucketedAttribute>> map = new HashMap<>();
        for (DCEncodedAttr encAtr : encAttrs) {
            String encAttrName = encAtr.getEncAttr();
            List<BucketedAttribute> bktAttrList = new ArrayList<>();
            for (DCBucketedAttr bktAttr : encAtr.getBktAttrs()) {
                bktAttr.setBuckets(bktAttr.getBucketAlgo().generateLabels());
                bktAttr.setDecodedStrategy(null);
                bktAttr.setBucketAlgo(null);
                bktAttr.setSourceAttr(null);
                bktAttrList.add(bktAttr);
            }
            map.put(encAttrName, bktAttrList);
        }
        return map;
    }

}
