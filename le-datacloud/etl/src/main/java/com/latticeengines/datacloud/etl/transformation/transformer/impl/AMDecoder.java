package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_AM_DECODER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.BitCodeBookUtils;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.etl.transformation.TransformerUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.AMDecoderParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMDecoderConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

@Component(AMDecoder.TRANSFORMER_NAME)
public class AMDecoder extends AbstractDataflowTransformer<AMDecoderConfig, AMDecoderParameters> {
    private static final Logger log = LoggerFactory.getLogger(AMDecoder.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_AM_DECODER;
    public static final String DATAFLOW_BEAN_NAME = "AMDecodeFlow";
    public static final String DATA_CLOUD_VERSION_NAME = "DataCloudVersion";

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private SourceAttributeEntityMgr srcAttrEntityMgr;

    @Value("${datacloud.etl.am.max.decode.num:100}")
    private int maxDecodeNum;

    @Override
    protected String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected boolean validateConfig(AMDecoderConfig config, List<String> sourceNames) {
        String error = null;
        if (config.getDecodeFields() == null) {
            error = "Attributes to decode are not given.";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }

        if (config.getDecodeFields().length > maxDecodeNum) {
            error = String.format(
                    "Too many attributes are given. Number of given attributes is %d but the maximum is %d.",
                    config.getDecodeFields().length, maxDecodeNum);
            log.error(error);
            RequestContext.logError(error);
            return false;
        }

        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return AMDecoderConfig.class;
    }

    @Override
    protected Class<AMDecoderParameters> getDataFlowParametersClass() {
        return AMDecoderParameters.class;
    }

    @Override
    protected void updateParameters(AMDecoderParameters parameters, Source[] baseTemplates, Source targetTemplate,
                                    AMDecoderConfig configuration, List<String> baseVersions) {
        parameters.setDecodeFields(configuration.getDecodeFields());
        parameters.setRetainFields(configuration.getRetainFields());
        Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(baseTemplates[0], baseVersions.get(0));
        if (schema == null) {
            String avroGlob = TransformerUtils.avroPath(baseTemplates[0], baseVersions.get(0), hdfsPathBuilder);
            schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        }
        String dataCloudVersion = schema.getProp(AMDecoder.DATA_CLOUD_VERSION_NAME);
        if (dataCloudVersion == null) {
            throw new RuntimeException(String.format("Failed to extract data cloud version from schema. Schema is %s",
                    JsonUtils.serialize(schema)));
        }
        List<SourceAttribute> srcAttrs = srcAttrEntityMgr.getAttributes("AccountMasterDecoded",
                "DECODE", TRANSFORMER_NAME, dataCloudVersion);
        Map<String, SourceAttribute> sourceAttributeMap = new HashMap<>();
        for (SourceAttribute attrib : srcAttrs) {
            sourceAttributeMap.put(attrib.getAttribute(), attrib);
        }
        Map<String, BitCodeBook> codeBookMap = new TreeMap<>();
        Map<String, String> codeBookLookup = new TreeMap<>();
        Map<String, String> decodeStrs = new HashMap<>();

        for (String field : parameters.getDecodeFields()) {
            SourceAttribute attrib = sourceAttributeMap.get(field);
            if (attrib == null) {
                throw new RuntimeException(String.format("Given attribute %s is not decodable.", field));
            }
            decodeStrs.put(attrib.getAttribute(), attrib.getArguments());
        }
        BitCodeBookUtils.constructCodeBookMap(codeBookMap, codeBookLookup, decodeStrs);
        parameters.setCodeBookMap(codeBookMap);
        parameters.setCodeBookLookup(codeBookLookup);
    }
}
