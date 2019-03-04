package com.latticeengines.datacloud.etl.transformation.transformer.impl.am;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_AM_DECODER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractDataflowTransformer;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.MapAttributeTransformer;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.am.AMDecoderParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.am.AMDecoderConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

@Component(AMDecoder.TRANSFORMER_NAME)
public class AMDecoder extends AbstractDataflowTransformer<AMDecoderConfig, AMDecoderParameters> {
    private static final Logger log = LoggerFactory.getLogger(AMDecoder.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_AM_DECODER;
    public static final String DATAFLOW_BEAN_NAME = "AMDecodeFlow";
    public static final String DATA_CLOUD_VERSION_NAME = "DataCloudVersion";

    @Inject
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
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
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return AMDecoderConfig.class;
    }

    @Override
    protected Class<AMDecoderParameters> getDataFlowParametersClass() {
        return AMDecoderParameters.class;
    }

    @Override
    protected boolean validateConfig(AMDecoderConfig config, List<String> baseSources) {
        String error = null;
        if (config.isDecodeAll()) {
            return true;
        }
        if (ArrayUtils.isEmpty(config.getDecodeFields())) {
            error = "Please provide attributes to decode.";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        return true;
    }

    @Override
    protected void updateParameters(AMDecoderParameters parameters, Source[] baseTemplates, Source targetTemplate,
                                    AMDecoderConfig configuration, List<String> baseVersions) {
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

        if (dataCloudVersion.contains(MapAttributeTransformer.MINI_PREFIX)) {
            dataCloudVersion = dataCloudVersion.substring(MapAttributeTransformer.MINI_PREFIX.length());
        }

        List<SourceAttribute> srcAttrs = srcAttrEntityMgr.getAttributes("AccountMasterDecoded",
                "DECODE", TRANSFORMER_NAME, dataCloudVersion, false);
        if (!configuration.isDecodeAll()) {
            parameters.setDecodeFields(configuration.getDecodeFields());
            if (ArrayUtils.isEmpty(configuration.getRetainFields())) {
                String[] retainFields = { //
                        DataCloudConstants.LATTICE_ID, //
                        DataCloudConstants.ATTR_LDC_DOMAIN, //
                        DataCloudConstants.ATTR_LDC_DUNS, //
                };
                parameters.setRetainFields(retainFields);
            } else {
                parameters.setRetainFields(configuration.getRetainFields());
            }
        } else {
            List<String> decodeFields = srcAttrs.stream() //
                    .filter(srcAttr -> StringUtils.isNotBlank(srcAttr.getArguments())) //
                    .map(SourceAttribute::getAttribute).collect(Collectors.toList());
            parameters.setDecodeFields(decodeFields.toArray(new String[decodeFields.size()]));
            // If configuration.getRetainFields() is empty, in dataflow
            // AMDecode, all plain attributes will be retained
            parameters.setRetainFields(configuration.getRetainFields());
        }
        parameters.setDecodeAll(configuration.isDecodeAll());

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
