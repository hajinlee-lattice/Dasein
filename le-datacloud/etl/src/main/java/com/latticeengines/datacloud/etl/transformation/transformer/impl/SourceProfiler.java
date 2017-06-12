package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.transformation.Profile;
import com.latticeengines.datacloud.dataflow.utils.FileParser;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategorizedIntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

import edu.emory.mathcs.backport.java.util.Arrays;

@Component(Profile.TRANSFORMER_NAME)
public class SourceProfiler extends AbstractDataflowTransformer<ProfileConfig, ProfileParameters> {
    private static final Log log = LogFactory.getLog(SourceProfiler.class);

    private static final String STRATEGY_ENCODED_COLUMN = "EncodedColumn";

    @Value("${datacloud.etl.profile.encode.bit:64}")
    private int encodeBits;

    @Value("${datacloud.etl.profile.attrs:1000}")
    private int maxAttrs;

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
    }

   @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir, ProfileParameters parameters,
                                         ProfileConfig configuration) {
       List<String> idAttrs = new ArrayList<>();
        List<ProfileParameters.Attribute> numericAttrs = new ArrayList<>();
        List<ProfileParameters.Attribute> retainedAttrs = new ArrayList<>();
        List<ProfileParameters.Attribute> encodedAttrs = new ArrayList<>();
        classifyAttrs(step.getBaseSources()[0], step.getBaseVersions().get(0), idAttrs, numericAttrs, retainedAttrs, encodedAttrs);
        if (idAttrs.size() != 1) {
            throw new RuntimeException(
                    "One and only one lattice account id is required. Allowed id field: LatticeAccountId, LatticeID");
        }
        parameters.setNumericAttrs(numericAttrs);
        parameters.setRetainedAttrs(retainedAttrs);
        parameters.setEncodedAttrs(encodedAttrs);
        parameters.setIdAttr(idAttrs.get(0));
    }

    @SuppressWarnings("unchecked")
    private void classifyAttrs(Source baseSrc, String baseVer, List<String> idAttrs,
            List<ProfileParameters.Attribute> numericAttrs, List<ProfileParameters.Attribute> retainedAttrs,
            List<ProfileParameters.Attribute> encodedAttrs) {
        // TODO: Will replace by reading from SourceAttribute table
        Map<String, Map<String, Object>> amAttrConf = FileParser.parseAMProfileConfig();
        Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(baseSrc, baseVer);
        log.info("Classifying attributes...");
        Set<String> numTypes = new HashSet<>(Arrays.asList(new String[] { "int", "long", "float", "double" }));
        Set<String> boolTypes = new HashSet<>(Arrays.asList(new String[] { "boolean" }));
        Map<String, List<ProfileParameters.Attribute>> attrsToDecode = new HashMap<>();
        // Attributes need to decode
        for (Map.Entry<String, Map<String, Object>> conf : amAttrConf.entrySet()) {
            if (StringUtils.isBlank((String) conf.getValue().get(FileParser.AM_PROFILE_CONFIG_HEADER[2])) || !((Boolean) conf.getValue().get(FileParser.AM_PROFILE_CONFIG_HEADER[1]))) {
                continue;
            }
            String decodeStrategy = (String) conf.getValue().get(FileParser.AM_PROFILE_CONFIG_HEADER[2]);
            String attrToDecode = null;
            try {
                JsonNode strategy = om.readTree(decodeStrategy);
                attrToDecode = strategy.get(STRATEGY_ENCODED_COLUMN).asText();
                if (!attrsToDecode.containsKey(attrToDecode)) {
                    attrsToDecode.put(attrToDecode, new ArrayList<>());
                }
            } catch (IOException e) {
                throw new RuntimeException("Fail to parse decodeStrategy in json format: " + decodeStrategy, e);
            }
            Integer encodeBitUnit = conf.getValue().get(FileParser.AM_PROFILE_CONFIG_HEADER[3]) == null ? null
                    : (Integer) (conf.getValue().get(FileParser.AM_PROFILE_CONFIG_HEADER[3]));
            BucketAlgorithm algo = parseBucketAlgo(
                    (String) conf.getValue().get(FileParser.AM_PROFILE_CONFIG_HEADER[4]));
            attrsToDecode.get(attrToDecode)
                    .add(new ProfileParameters.Attribute(conf.getKey(), encodeBitUnit, decodeStrategy, algo));
        }
        // Attributes exist in schema of source to profile
        for (Field field : schema.getFields()) {
            if (field.name().equals(DataCloudConstants.LATTIC_ID)
                    || field.name().equals(DataCloudConstants.LATTICE_ACCOUNT_ID)) {    // lattice account id
                log.info(String.format("ID attr: %s (unencoded)", field.name()));
                idAttrs.add(field.name());
                continue;
            }
            boolean isSeg = false;
            if (DataCloudConstants.PROFILE_ATTR_SRC_CUSTOMER
                    .equals(field.getProp(DataCloudConstants.PROFILE_ATTR_SRC))) { // from customer table
                isSeg = true;
            } else if (amAttrConf.containsKey(field.name())) { // from AM attr
                isSeg = (Boolean) amAttrConf.get(field.name()).get(FileParser.AM_PROFILE_CONFIG_HEADER[1]);
            } else if (attrsToDecode.containsKey(field.name())) { // from encoded AM attr
                isSeg = true;
            }
            if (!isSeg) { // If not for segment, exclude it from profiling
                log.info(String.format("Discarded attr: %s", field.name()));
                continue;
            }
            if (attrsToDecode.containsKey(field.name())) {
                for (ProfileParameters.Attribute attr : attrsToDecode.get(field.name())) {
                    if (attr.getAlgo() instanceof BooleanBucket) {
                        log.info(String.format("%s attr %s (encoded)", attr.getAlgo().getClass().getSimpleName(),
                                attr.getAttr()));
                        encodedAttrs.add(attr);
                    } else if (attr.getAlgo() instanceof IntervalBucket) {
                        log.info(String.format("%s attr %s (unencoded)", attr.getAlgo().getClass().getSimpleName(),
                                attr.getAttr()));
                        numericAttrs.add(attr);
                    } else {
                        log.info(String.format("%s attr %s (unencoded)", attr.getAlgo().getClass().getSimpleName(),
                                attr.getAttr()));
                        retainedAttrs.add(attr);
                    }
                }
                continue;
            }
            String type = field.schema().getTypes().get(0).getName();
            if (numTypes.contains(type)) {
                log.info(String.format("Interval bucketed attr %s (type %s unencoded)", field.name(), type));
                numericAttrs.add(new ProfileParameters.Attribute(field.name(), null, null, new IntervalBucket()));
                continue;
            }
            if (boolTypes.contains(type)) {
                log.info(String.format("Boolean bucketed attr %s (type %s encoded)", field.name(), type));
                BucketAlgorithm algo = new BooleanBucket();
                encodedAttrs.add(new ProfileParameters.Attribute(field.name(), 2, null, algo));
                continue;
            }
            log.info(String.format("Retained attr: %s (unencoded)", field.name()));
            retainedAttrs.add(new ProfileParameters.Attribute(field.name(), null, null, null));
        }
    }

    private BucketAlgorithm parseBucketAlgo(String algo) {
        if (StringUtils.isBlank(algo)) {
            return null;
        }
        if (BooleanBucket.class.getSimpleName().equalsIgnoreCase(algo)) {
            return new BooleanBucket();
        }
        if (CategoricalBucket.class.getSimpleName().equalsIgnoreCase(algo)) {
            return new CategoricalBucket();
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
    protected void postDataFlowProcessing(String workflowDir, ProfileParameters para, ProfileConfig config) {
        List<Object[]> result = new ArrayList<>();
        result.add(profileIdAttr(para.getIdAttr()));
        for (ProfileParameters.Attribute attr : para.getRetainedAttrs()) {
            result.add(profileRetainedAttr(attr));
        }
        Map<String, List<ProfileParameters.Attribute>> encodedAttrs = groupEncodedAttrs(para.getEncodedAttrs());
        int size = result.size() + para.getNumericAttrs().size() + encodedAttrs.size();
        log.info(String.format("1 LatticeAccountId attr, %d numeric attrs, %d encoded attrs, %d retained attrs",
                para.getNumericAttrs().size(), encodedAttrs.size(), para.getRetainedAttrs().size()));
        if (size > maxAttrs) {
            log.warn(String.format("Attr num in bucketed am %d exceeds expected maximum limit %d", size, maxAttrs));
        }
        for (Map.Entry<String, List<ProfileParameters.Attribute>> entry : encodedAttrs.entrySet()) {
            int lowestBit = 0;
            for (ProfileParameters.Attribute attr : entry.getValue()) {
                result.add(profileEncodedAttr(attr, entry.getKey(), lowestBit));
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
            AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, targetDir, "AMProfileAdditional.avro");
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

    private Object[] profileRetainedAttr(ProfileParameters.Attribute attr) {
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

    private Object[] profileEncodedAttr(ProfileParameters.Attribute attr, String encodedAttr, int lowestBit) {
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

    private Map<String, List<ProfileParameters.Attribute>> groupEncodedAttrs(List<ProfileParameters.Attribute> attrs) {
        Collections.sort(attrs, (x, y) -> y.getEncodeBitUnit().compareTo(x.getEncodeBitUnit()));    // descending order
        Map<String, List<ProfileParameters.Attribute>> encodedAttrs = new HashMap<>();
        List<Map<String, List<ProfileParameters.Attribute>>> availableBits = new ArrayList<>(); // 0 - 63
        for (int i = 0; i < encodeBits; i++) {
            availableBits.add(new HashMap<>());
        }
        for (ProfileParameters.Attribute attr : attrs) {
            if (attr.getEncodeBitUnit() == null || attr.getEncodeBitUnit() <= 0 || attr.getEncodeBitUnit() > encodeBits) {
                throw new RuntimeException(
                        String.format("Attribute %s EncodeBitUnit %d is not in range [1, %d]",
                                attr.getAttr(), attr.getEncodeBitUnit(), encodeBits));
            }
            int index = attr.getEncodeBitUnit();
            while (index < encodeBits && availableBits.get(index).size() == 0) {
                index++;
            }
            String encodedAttr = null;
            List<ProfileParameters.Attribute> attachedAttrs = null;
            if (index == encodeBits) {  // No available encode attr to add this attr. Add a new encode attr
                encodedAttr = createEncodeAttrName();
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

    private String createEncodeAttrName() {
        return "EAttr_" + UUID.randomUUID().toString().replace("-", "_");
    }
}
