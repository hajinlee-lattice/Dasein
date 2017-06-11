package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.transformation.Profile;
import com.latticeengines.datacloud.dataflow.utils.FileParser;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

import edu.emory.mathcs.backport.java.util.Arrays;

@Component(Profile.TRANSFORMER_NAME)
public class ProfileTransformer extends AbstractDataflowTransformer<ProfileConfig, ProfileParameters> {
    private static final Log log = LogFactory.getLog(ProfileTransformer.class);

    private static final String STRATEGY_ENCODED_COLUMN = "EncodedColumn";
    private static final String STRATEGY_BIT_INTERPRETATION = "BitInterpretation";
    private static final String STRATEGY_BIT_UNIT = "BitUnit";
    private static final String BOOLEAN_YESNO = "BOOLEAN_YESNO";

    private ObjectMapper om = new ObjectMapper();

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Override
    protected String getDataFlowBeanName() {
        return Profile.BEAN_NAME;
    }

    @Override
    public String getName() {
        return Profile.TRANSFORMER_NAME;
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
    protected void updateParameters(ProfileParameters parameters, Source[] baseTemplates, Source targetTemplate,
            ProfileConfig config, List<String> baseVersions) {
        parameters.setNumBucketEqualSized(config.isNumBucketEqualSized());
        parameters.setBucketNum(config.getBucketNum());
        parameters.setMinBucketSize(config.getMinBucketSize());
        parameters.setRandSeed(config.getRandSeed());
        List<String> numAttrs = new ArrayList<>();
        List<String> boolAttrs = new ArrayList<>();
        Map<String, Pair<Integer, Map<String, String>>> encodedAttrs = new HashMap<>();
        List<String> retainedAttrs = new ArrayList<>();
        List<String> idAttrs = new ArrayList<>();
        classifyAttrs(baseTemplates[0], baseVersions.get(0), numAttrs, boolAttrs, encodedAttrs, retainedAttrs, idAttrs);
        if (idAttrs.size() != 1) {
            throw new RuntimeException(
                    "One and only one lattice account id is required. Allowed id field: LatticeAccountId, LatticeID");
        }
        parameters.setNumAttrs(numAttrs);
        parameters.setBoolAttrs(boolAttrs);
        parameters.setEncodedAttrs(encodedAttrs);
        parameters.setRetainedAttrs(retainedAttrs);
        parameters.setIdAttr(idAttrs.get(0));
    }

    @SuppressWarnings("unchecked")
    private void classifyAttrs(Source baseSrc, String baseVer, List<String> numAttrs, List<String> boolAttrs,
            Map<String, Pair<Integer, Map<String, String>>> encodedAttrs, List<String> retainedAttrs,
            List<String> idAttrs) {
        // TODO: Will replace by reading from SourceAttribute table
        Map<String, Map<String, Object>> amAttrConf = FileParser.parseAMProfileConfig();
        Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(baseSrc, baseVer);
        log.info("Classifying attributes...");
        String[] numTypeArr = {"int", "long", "float", "double"};
        String[] boolTypeArr = {"boolean"};
        Set<String> numTypes = new HashSet<>(Arrays.asList(numTypeArr));
        Set<String> boolTypes = new HashSet<>(Arrays.asList(boolTypeArr));
        Map<String, Pair<Integer, Map<String, String>>> encodedAttrsPre = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> conf : amAttrConf.entrySet()) {
            if (StringUtils.isNotBlank((String) conf.getValue().get(FileParser.AM_PROFILE_CONFIG_HEADER[3]))) { // decode strategy
                String decodeStrategy = (String) conf.getValue().get(FileParser.AM_PROFILE_CONFIG_HEADER[3]);
                try {
                    JsonNode strategy = om.readTree(decodeStrategy);
                    String encodedAttr = strategy.get(STRATEGY_ENCODED_COLUMN).asText();
                    String bitInterpretation = strategy.get(STRATEGY_BIT_INTERPRETATION).asText();
                    Integer bitUnit = strategy.get(STRATEGY_BIT_UNIT) != null
                            ? Integer.valueOf(strategy.get(STRATEGY_BIT_UNIT).asText()) : null;
                    if (bitUnit == null && BOOLEAN_YESNO.equals(bitInterpretation)) {
                        bitUnit = 2;
                    }
                    if (bitUnit == null) {
                        bitUnit = 1;
                    }
                    if (!encodedAttrsPre.containsKey(encodedAttr)) {
                        encodedAttrsPre.put(encodedAttr, Pair.of(bitUnit, new HashMap<>()));
                    }
                    encodedAttrsPre.get(encodedAttr).getRight().put(conf.getKey(), decodeStrategy);
                } catch (IOException e) {
                    throw new RuntimeException("Fail to parse decodeStrategy in json format: " + decodeStrategy, e);
                }

            }
        }
        for (Field field : schema.getFields()) {
            boolean isSeg = false, isBucket = false;
            if (DataCloudConstants.PROFILE_ATTR_SRC_CUSTOMER
                    .equals(field.getProp(DataCloudConstants.PROFILE_ATTR_SRC))) { // from customer
                isSeg = true;
                isBucket = true;
            } else if (amAttrConf.containsKey(field.name())) { // from AM
                isBucket = (Boolean) amAttrConf.get(field.name()).get(FileParser.AM_PROFILE_CONFIG_HEADER[1]);
                isSeg = (Boolean) amAttrConf.get(field.name()).get(FileParser.AM_PROFILE_CONFIG_HEADER[2]);
            } else if (encodedAttrsPre.containsKey(field.name())) { // from AM encoded
                isSeg = true;
                isBucket = true;
            }
            if (field.name().equals(DataCloudConstants.LATTIC_ID)
                    || field.name().equals(DataCloudConstants.LATTICE_ACCOUNT_ID)) {
                log.info(String.format("ID attr: %s", field.name()));
                idAttrs.add(field.name());
                continue;
            }
            if (!isSeg) { // If not for segment, exclude it from profiling
                log.info(String.format("Discarded attr: %s", field.name()));
                continue;
            }
            if (!isBucket) { // if for segment but not bucket
                log.info(String.format("Retained attr: %s", field.name()));
                retainedAttrs.add(field.name());
                continue;
            }
            String type = field.schema().getTypes().get(0).getName();
            if (numTypes.contains(type)) {
                log.info(String.format("Interval bucketed attr %s (%s)", field.name(), type));
                numAttrs.add(field.name());
                continue;
            }
            if (boolTypes.contains(type)) {
                log.info(String.format("Boolean bucketed attr %s (%s)", field.name(), type));
                boolAttrs.add(field.name());
                continue;
            }
            if (encodedAttrsPre.containsKey(field.name())) {
                Iterator<Entry<String, String>> iter = encodedAttrsPre.get(field.name()).getRight().entrySet()
                        .iterator();
                while (iter.hasNext()) {
                    String attr = iter.next().getKey();
                    isBucket = (Boolean) amAttrConf.get(attr).get(FileParser.AM_PROFILE_CONFIG_HEADER[1]);
                    isSeg = (Boolean) amAttrConf.get(attr).get(FileParser.AM_PROFILE_CONFIG_HEADER[2]);
                    if (!isSeg) {
                        log.info(String.format("Discarded attr: %s (from encoded attr %s)", attr, field.name()));
                        iter.remove();
                    } else {
                        log.info(String.format("Encoded bucketed attr %s", attr));
                    }
                }
                if (encodedAttrsPre.get(field.name()).getRight().size() > 0) {
                    encodedAttrs.put(field.name(), encodedAttrsPre.get(field.name()));
                }
                continue;
            }
            log.info(String.format("Retained attr: %s", field.name()));
            retainedAttrs.add(field.name());
        }
    }

    @Override
    protected void postDataFlowProcessing(String workflowDir, ProfileParameters para, ProfileConfig config) {
        int size = 1 + para.getBoolAttrs().size() + para.getRetainedAttrs().size(); // 1 is id attr
        for (Pair<Integer, Map<String, String>> encodedEnt : para.getEncodedAttrs().values()) {
            size += encodedEnt.getRight().size();
        }
        Object[][] data = new Object[size][7];
        int i = 0;
        profileIdAttr(para.getIdAttr(), data[i]);
        for (String attr : para.getRetainedAttrs()) {
            i++;
            profileRetainedAttrs(attr, data[i]);
        }
        int j = 0;
        for (String attr : para.getBoolAttrs()) {
            i++;
            profileBoolAttrs(attr, data[i], "EAttrBool", j, 2);
            j += 2;
        }
        Map<String, Pair<Integer, Map<String, String>>> encodedAttrs = para.getEncodedAttrs();
        for (Map.Entry<String, Pair<Integer, Map<String, String>>> encodedAttr : encodedAttrs.entrySet()) {
            String encodedAttrOri = encodedAttr.getKey();
            int bitUnit = encodedAttr.getValue().getLeft();
            j = 0;
            for (Map.Entry<String, String> attr : encodedAttr.getValue().getRight().entrySet()) {
                i++;
                profileEncodedAttrs(attr.getKey(), data[i], attr.getValue(), "EAttr_" + encodedAttrOri, j, bitUnit);
                j += bitUnit;
            }
        }
        List<Pair<String, Class<?>>> columns = prepareColumns();
        upload(workflowDir, columns, data);
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

    private void upload(String targetDir, List<Pair<String, Class<?>>> schema, Object[][] data) {
        try {
            AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, targetDir, "AMProfileAdditional.avro");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void profileIdAttr(String idAttr, Object[] arr) {
        arr[0] = DataCloudConstants.LATTICE_ACCOUNT_ID;
        arr[1] = idAttr;
        arr[2] = null;
        arr[3] = null;
        arr[4] = null;
        arr[5] = null;
        arr[6] = null;
    }

    private void profileRetainedAttrs(String attr, Object[] arr) {
        arr[0] = attr;
        arr[1] = attr;
        arr[2] = null;
        arr[3] = null;
        arr[4] = null;
        arr[5] = null;
        arr[6] = null;
    }

    private void profileBoolAttrs(String attr, Object[] arr, String encodedAttr, int lowestBit, int bitUnit) {
        arr[0] = attr;
        arr[1] = attr;
        arr[2] = null;
        arr[3] = encodedAttr;
        arr[4] = lowestBit;
        arr[5] = bitUnit;
        BooleanBucket bucket = new BooleanBucket();
        try {
            arr[6] = om.writeValueAsString(bucket);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Fail to format BooleanBucket object to json", e);
        }

    }

    private void profileEncodedAttrs(String attr, Object[] arr, String decodeStrategy, String encodedAttr,
            int lowestBit, int bitUnit) {
        arr[0] = attr;
        arr[1] = attr;
        arr[2] = decodeStrategy;
        arr[3] = null;
        arr[4] = null;
        arr[5] = null;
        arr[6] = null;
        try {
            JsonNode strategy = om.readTree(decodeStrategy);
            String bitInterpretation = strategy.get(STRATEGY_BIT_INTERPRETATION).asText();
            if (BOOLEAN_YESNO.equals(bitInterpretation)) {
                arr[3] = encodedAttr;
                arr[4] = lowestBit;
                arr[5] = bitUnit;
                BooleanBucket bucket = new BooleanBucket();
                arr[6] = om.writeValueAsString(bucket);
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse bucket strategy", e);
        }
    }
}
