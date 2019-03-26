package com.latticeengines.datacloud.etl.transformation.service.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceProfiler;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;

public class SourceProfileSegmentStageDeploymentTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(SourceProfileSegmentStageDeploymentTestNG.class);

    private static final long RAND_SEED = 0L;
    private static final String MATCH_SEGMENT_PROFILE = "MatchSegmentProfile";

    private GeneralSource source = new GeneralSource(MATCH_SEGMENT_PROFILE);

    @Inject
    private SourceAttributeEntityMgr srcAttrEntityMgr;

    private static final String MATCH_TABLE_NAME = "Fortune1000";
    private GeneralSource matchTable = new GeneralSource(MATCH_TABLE_NAME);

    private String dataCloudVersion;

    private ObjectMapper om = new ObjectMapper();

    @Test(groups = "deployment")
    public void testTransformation() {
        dataCloudVersion = versionEntityMgr.currentApprovedVersionAsString();
        prepareMatch();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmIntermediateSource(source, null);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("SourceProfiling");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        SourceTable sourceTable1 = new SourceTable(matchTable.getSourceName(),
                CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
        List<String> baseSources1 = Collections.singletonList(matchTable.getSourceName());
        step1.setBaseSources(baseSources1);
        Map<String, SourceTable> baseTables1 = new HashMap<>();
        baseTables1.put(matchTable.getSourceName(), sourceTable1);
        step1.setBaseTables(baseTables1);
        step1.setTransformer(TRANSFORMER_MATCH);
        step1.setConfiguration(getMatchConfig());

        TransformationStepConfig step2 = new TransformationStepConfig();
        List<Integer> inputSteps = new ArrayList<>(Collections.singletonList(0));
        step2.setInputSteps(inputSteps);
        step2.setTransformer(SourceProfiler.TRANSFORMER_NAME);
        step2.setTargetSource(source.getSourceName());
        String confParamStr2 = getMatchSegmentProfileConfig();
        step2.setConfiguration(setDataFlowEngine(confParamStr2, "TEZ"));

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);
        steps.add(step2);

        // -----------
        configuration.setSteps(steps);
        configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
        configuration.setKeepTemp(true);
        return configuration;
    }

    private String getMatchConfig() {
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_CUSTOMERSPACE));
        UnionSelection us = new UnionSelection();
        Map<Predefined, String> ps = new HashMap<>();
        ps.put(Predefined.Segment, "2.0");
        ColumnSelection cs = new ColumnSelection();
        List<Column> cols = Arrays.asList(new Column(DataCloudConstants.ATTR_LDC_DOMAIN),
                new Column(DataCloudConstants.ATTR_LDC_NAME));
        cs.setColumns(cols);
        us.setPredefinedSelections(ps);
        us.setCustomSelection(cs);
        matchInput.setUnionSelection(us);
        matchInput.setKeyMap(getKeyMap());
        matchInput.setDataCloudVersion(dataCloudVersion);
        matchInput.setSkipKeyResolution(true);
        matchInput.setSplitsPerBlock(40);
        config.setMatchInput(matchInput);
        return JsonUtils.serialize(config);
    }

    private String getMatchSegmentProfileConfig() {
        ProfileConfig conf = new ProfileConfig();
        conf.setNumBucketEqualSized(false);
        conf.setBucketNum(4);
        conf.setMinBucketSize(2);
        conf.setRandSeed(RAND_SEED);
        conf.setMaxCat(10);
        conf.setDataCloudVersion(dataCloudVersion);
        return setDataFlowEngine(JsonUtils.serialize(conf), "TEZ");
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new TreeMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        return keyMap;
    }

    private Object[][] matchData = new Object[][] { //
            { "1", "google.com", null }, //
            { "2", "amazon.com", null }, //
            { "3", "baidu.com", null }, //
            { "4", "facebook.com", null }, //
            { "5", "lattice-engines.com", null }, //
            { "6", "uber.com", null }, //
            { "7", "linkedin.com", null }, //
            { "8", "twitter.com", null }, //
            { "9", "sina.com", null }, //
            { "10", "airbnb.com", null }, //
            { "11", "youtube.com", null }, //
            { "12", "wikipedia.org", null }, //
            { "13", "reddit.com", null }, //
            { "14", "qq.com", null }, //
            { "15", "taobao.com", null }, //
    };

    private void prepareMatch() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("ID", String.class));
        schema.add(Pair.of("Domain", String.class));
        schema.add(Pair.of("acct_int", Integer.class));
        uploadAndRegisterTableSource(schema, matchData, matchTable.getSourceName());
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        log.info(String.format("Start to verify intermediate source %s", source));
        try {
            verifyMatchSegmentProfileResult(records);
        } catch (Exception ex) {
            throw new RuntimeException("Exception in verifyIntermediateResult", ex);
        }
    }

    private Map<String, BucketAlgorithm> getExpectedBuckAlgoForSegment() {
        Map<String, BucketAlgorithm> map = new HashMap<>();
        DiscreteBucket disBucket = new DiscreteBucket();
        Number[] intValues = { 0, 1, 2 };
        disBucket.setValues(Arrays.asList(intValues));
        map.put("BusinessTechnologiesSeoMeta", disBucket);
        CategoricalBucket catBucket = new CategoricalBucket();
        String[] catValues = { "684", "425", "636", "716" };
        catBucket.setCategories(Arrays.asList(catValues));
        map.put("GLOBAL_ULTIMATE_DnB_COUNTY_CODE", catBucket);
        // map.put("LAST_UPDATE_DATE", null);
        map.put("acct_int", new IntervalBucket());
        return map;
    }

    private void verifyMatchSegmentProfileResult(Iterator<GenericRecord> records) throws IOException {
        Map<String, BucketAlgorithm> flatAttrsToCheck = getExpectedBuckAlgoForSegment();
        List<SourceAttribute> srcAttrs = srcAttrEntityMgr.getAttributes(SourceProfiler.AM_PROFILE,
                DataCloudConstants.PROFILE_STAGE_SEGMENT, DataCloudConstants.TRANSFORMER_PROFILER,
                dataCloudVersion, true);
        Map<String, String> decAttrToEnc = new HashMap<>();
        for (SourceAttribute srcAttr : srcAttrs) {
            JsonNode arg = om.readTree(srcAttr.getArguments());
            if (arg.get(SourceProfiler.IS_PROFILE).asBoolean() && arg.hasNonNull(SourceProfiler.DECODE_STRATEGY)) {
                if (!arg.hasNonNull(SourceProfiler.DECODE_STRATEGY)) {
                    decAttrToEnc.put(srcAttr.getAttribute(), null);
                    continue;
                }
                String decStr = arg.get(SourceProfiler.DECODE_STRATEGY).toString();
                BitDecodeStrategy bitDecodeStrategy = JsonUtils.deserialize(decStr, BitDecodeStrategy.class);
                Assert.assertNotNull(bitDecodeStrategy.getEncodedColumn());
                decAttrToEnc.put(srcAttr.getAttribute(), bitDecodeStrategy.getEncodedColumn());
            }
        }

        while (records.hasNext()) {
            GenericRecord record = records.next();
            Object attr = record.get("AttrName");
            Assert.assertNotNull(attr);
            if (attr instanceof Utf8) {
                attr = attr.toString();
            }
            Object bktAlgo = record.get(SourceProfiler.BKT_ALGO);
            if (bktAlgo instanceof Utf8) {
                bktAlgo = bktAlgo.toString();
            }

            Assert.assertTrue(attr instanceof String);
            if (decAttrToEnc.containsKey(attr)) { // Decoded attrs
                switch (decAttrToEnc.get(attr)) {
                case "HGData_SupplierTechIndicators":
                case "HGData_SegmentTechIndicators":
                case "BuiltWith_TechIndicators":
                    Assert.assertNotNull(bktAlgo);
                    BooleanBucket boolAlgo = JsonUtils.deserialize((String) bktAlgo, BooleanBucket.class);
                    Assert.assertNotNull(boolAlgo);
                    Assert.assertNotNull(record.get(DataCloudConstants.PROFILE_ATTR_ENCATTR));
                    Assert.assertNotNull(record.get(DataCloudConstants.PROFILE_ATTR_LOWESTBIT));
                    Assert.assertNotNull(record.get(DataCloudConstants.PROFILE_ATTR_NUMBITS));
                    break;
                case "BmbrSurge_Intent":
                    Assert.assertNotNull(bktAlgo);
                    CategoricalBucket catAlgo = JsonUtils.deserialize((String) bktAlgo, CategoricalBucket.class);
                    Assert.assertNotNull(catAlgo);
                    Assert.assertTrue(CollectionUtils.isNotEmpty(catAlgo.getCategories()));
                    Assert.assertEquals(String.join(",", catAlgo.generateLabels()), "null,Normal,Moderate,High");
                    Assert.assertNotNull(record.get(DataCloudConstants.PROFILE_ATTR_ENCATTR));
                    Assert.assertNotNull(record.get(DataCloudConstants.PROFILE_ATTR_LOWESTBIT));
                    Assert.assertNotNull(record.get(DataCloudConstants.PROFILE_ATTR_NUMBITS));
                    break;
                case "BmbrSurge_CompositeScore":
                    if (bktAlgo == null) {
                        continue;
                    }
                    boolean pass = false;
                    try {
                        IntervalBucket intAlgo = JsonUtils.deserialize((String) bktAlgo, IntervalBucket.class);
                        Assert.assertNotNull(intAlgo);
                        pass = true;
                    } catch (Exception ex) {
                        log.warn("Error verifying interval bucket: " + bktAlgo);
                    }
                    try {
                        DiscreteBucket intAlgo = JsonUtils.deserialize((String) bktAlgo, DiscreteBucket.class);
                        Assert.assertNotNull(intAlgo);
                        pass = true;
                    } catch (Exception ex) {
                        log.warn("Error verifying discrete bucket: " + bktAlgo);
                    }
                    Assert.assertTrue(pass);
                    break;
                case "BmbrSurge_BucketCode":
                    Assert.assertNotNull(bktAlgo);
                    catAlgo = JsonUtils.deserialize((String) bktAlgo, CategoricalBucket.class);
                    Assert.assertNotNull(catAlgo);
                    Assert.assertTrue(CollectionUtils.isNotEmpty(catAlgo.getCategories()));
                    Assert.assertEquals(String.join(",", catAlgo.generateLabels()), "null,A,B,C");
                    Assert.assertNotNull(record.get(DataCloudConstants.PROFILE_ATTR_ENCATTR));
                    Assert.assertNotNull(record.get(DataCloudConstants.PROFILE_ATTR_LOWESTBIT));
                    Assert.assertNotNull(record.get(DataCloudConstants.PROFILE_ATTR_NUMBITS));
                    break;
                default: // deprecated decoded attrs
                    break;
                }
            } else { // Flat attributes
                if (bktAlgo == null) {
                    log.info("Empty Algo:" + record.toString());
                }
                if (!flatAttrsToCheck.containsKey(attr)) {
                    continue;
                }
                if (flatAttrsToCheck.get(attr) == null) { // Retained attributes
                    Assert.assertNull(bktAlgo);
                } else {
                    log.info("Checking flat attr: " + attr);
                    if (flatAttrsToCheck.get(attr) instanceof IntervalBucket) {
                        IntervalBucket actual = JsonUtils.deserialize((String) bktAlgo, IntervalBucket.class);
                        Assert.assertNotNull(actual);
                        IntervalBucket expected = (IntervalBucket) (flatAttrsToCheck.get(attr));
                        if (CollectionUtils.isNotEmpty(expected.getBoundaries())) {
                            Assert.assertTrue(CollectionUtils.isNotEmpty(actual.getBoundaries()));
                            Assert.assertNotNull(actual.getBoundaries(), String.valueOf(bktAlgo));
                            Assert.assertEquals(actual.getBoundaries().size(), expected.getBoundaries().size(), //
                                    String.valueOf(bktAlgo));
                            for (int i = 0; i < actual.getBoundaries().size(); i++) {
                                Assert.assertEquals(actual.getBoundaries().get(i), expected.getBoundaries().get(i), //
                                        String.valueOf(bktAlgo));
                            }
                        }
                    } else if (flatAttrsToCheck.get(attr) instanceof DiscreteBucket) {
                        DiscreteBucket actual = JsonUtils.deserialize((String) bktAlgo, DiscreteBucket.class);
                        Assert.assertNotNull(actual);
                        DiscreteBucket expected = (DiscreteBucket) (flatAttrsToCheck.get(attr));
                        Assert.assertEquals( //
                                CollectionUtils.size(actual.getValues()), CollectionUtils.size(expected.getValues()), //
                                String.valueOf(bktAlgo));
                        if (CollectionUtils.isNotEmpty(actual.getValues())) {
                            for (int i = 0; i < actual.getValues().size(); i++) {
                                Assert.assertEquals(actual.getValues().get(i), expected.getValues().get(i), //
                                        String.valueOf(bktAlgo));
                            }
                        }
                    } else if (flatAttrsToCheck.get(attr) instanceof CategoricalBucket) {
                        CategoricalBucket actual = JsonUtils.deserialize((String) bktAlgo, CategoricalBucket.class);
                        Assert.assertNotNull(actual);
                        CategoricalBucket expected = (CategoricalBucket) (flatAttrsToCheck.get(attr));
                        Assert.assertNotNull(actual.getCategories(), String.valueOf(bktAlgo));
                        Assert.assertEquals(actual.getCategories().size(), expected.getCategories().size(), //
                                String.valueOf(bktAlgo));
                        for (int i = 0; i < actual.getCategories().size(); i++) {
                            Assert.assertEquals(actual.getCategories().get(i), expected.getCategories().get(i), //
                                    String.valueOf(bktAlgo));
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

}
