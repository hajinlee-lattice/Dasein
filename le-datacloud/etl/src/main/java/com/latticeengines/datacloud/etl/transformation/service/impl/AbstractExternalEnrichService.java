package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.etl.transformation.service.ExternalEnrichService;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.ExternalEnrichRequest;

public abstract class AbstractExternalEnrichService implements ExternalEnrichService {

    private static final Logger log = LoggerFactory.getLogger(AbstractExternalEnrichService.class);

    @Autowired
    protected Configuration yarnConfiguration;

    @Override
    public void enrich(ExternalEnrichRequest request) {
        String inputAvroGlob = request.getAvroInputGlob();
        Schema inputSchema = AvroUtils.getSchemaFromGlob(yarnConfiguration, inputAvroGlob);
        validateInputAndUpdateKeyMap(request, inputSchema);
        Schema outputSchema = constructOutputSchema(request.getRecordName(), request.getOutputKeyMapping(),
                inputSchema);
        enrichAndWriteTo(request.getAvroOuptutDir(), outputSchema, inputAvroGlob, request.getInputKeyMapping());
    }

    private void validateInputAndUpdateKeyMap(ExternalEnrichRequest request, Schema inputSchema) {
        log.info("Validating input ...");
        List<String> fieldNames = new ArrayList<>();
        for (Schema.Field field : inputSchema.getFields()) {
            fieldNames.add(field.name());
        }

        Map<MatchKey, List<String>> inputkeyMapping = resolveKeyMap(request, fieldNames);
        for (MatchKey key : requiredInputKeys()) {
            if (!inputkeyMapping.keySet().contains(key)) {
                throw new IllegalArgumentException("Cannot find required input key " + key);
            }
        }
        request.setInputKeyMapping(inputkeyMapping);
    }

    private static Map<MatchKey, List<String>> resolveKeyMap(ExternalEnrichRequest request, List<String> fieldNames) {
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(fieldNames);
        Map<MatchKey, List<String>> inputkeyMapping = request.getInputKeyMapping();
        if (inputkeyMapping != null && !inputkeyMapping.keySet().isEmpty()) {
            for (Map.Entry<MatchKey, List<String>> entry : inputkeyMapping.entrySet()) {
                log.debug("Overwriting key map entry " + JsonUtils.serialize(entry));
                keyMap.put(entry.getKey(), entry.getValue());
            }
        }

        for (List<String> fields : keyMap.values()) {
            if (fields != null && !fields.isEmpty()) {
                for (String field : fields) {
                    if (!fieldNames.contains(field)) {
                        throw new IllegalArgumentException(
                                "Cannot find target field " + field + " in claimed field list.");
                    }
                }
            }
        }

        return keyMap;
    }

    private Schema constructOutputSchema(String recordName, Map<MatchKey, String> outputKeyMap, Schema inputSchema) {
        List<String> inputFields = new ArrayList<>();
        for (Schema.Field field : inputSchema.getFields()) {
            inputFields.add(field.name());
        }

        List<String> additionalFields = new ArrayList<>(outputKeyMap.values());
        additionalFields.removeAll(inputFields);

        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(recordName);
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        SchemaBuilder.FieldBuilder<Schema> fieldBuilder;
        for (String fieldName : additionalFields) {
            fieldBuilder = fieldAssembler.name(StringUtils.strip(fieldName));
            Schema.Type type = AvroUtils.getAvroType(String.class);
            AvroUtils.constructFieldWithType(fieldAssembler, fieldBuilder, type);
        }
        Schema additionalSchema = fieldAssembler.endRecord();
        return (Schema) AvroUtils.combineSchemas(additionalSchema, inputSchema)[0];
    }

    protected abstract Set<MatchKey> requiredInputKeys();

    protected abstract void enrichAndWriteTo(String ouputDir, Schema outputSchema, String inputAvroGlob,
            Map<MatchKey, List<String>> inputKeyMap);

}
