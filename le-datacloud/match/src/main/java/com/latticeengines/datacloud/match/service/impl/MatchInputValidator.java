package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.IOBufferType;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public class MatchInputValidator {
    private static Logger log = LoggerFactory.getLogger(MatchInputValidator.class);

    public static void validateRealTimeInput(MatchInput input, int maxRealTimeInput) {
        Map<MatchKey, List<String>> keyMap = commonValidation(input);
        input.setKeyMap(keyMap);

        validateKeys(input.getKeyMap().keySet());

        if (input.getData() == null || input.getData().isEmpty()) {
            throw new IllegalArgumentException("Empty input data.");
        }

        if (input.getData().size() > maxRealTimeInput) {
            throw new IllegalArgumentException("Too many input data, maximum rows = " + maxRealTimeInput);
        }
    }

    public static void validateBulkInput(MatchInput input, Configuration yarnConfiguration) {
        if (input.getInputBuffer() == null) {
            throw new IllegalArgumentException("Bulk input must have an IO buffer.");
        }

        if (input.getOutputBufferType() == null) {
            log.info("Output buffer type is unset, using the input buffer type "
                    + input.getInputBuffer().getBufferType() + " as default.");
            input.setOutputBufferType(input.getInputBuffer().getBufferType());
        }

        if (IOBufferType.SQL.equals(input.getOutputBufferType())) {
            throw new UnsupportedOperationException("Only the IOBufferType [AVRO] is supported");
        }

        List<String> inputFields;
        switch (input.getInputBuffer().getBufferType()) {
        case AVRO:
            inputFields = validateInputAvroAndGetFieldNames(input.getInputBuffer(), yarnConfiguration);
            break;
        case SQL:
            throw new UnsupportedOperationException("SQL buffer has not been implemented yet.");
        default:
            throw new IllegalArgumentException(
                    "Unknown type of input buffer " + input.getInputBuffer().getBufferType());
        }
        input.setFields(inputFields);

        Map<MatchKey, List<String>> keyMap = commonValidation(input);
        input.setKeyMap(keyMap);
        validateKeys(input.getKeyMap().keySet());
    }

    private static Map<MatchKey, List<String>> commonValidation(MatchInput input) {
        if (input.getTenant() == null) {
            throw new IllegalArgumentException("Must provide tenant to run a match.");
        }
        validateColumnSelection(input);

        if (input.getFields() == null || input.getFields().isEmpty()) {
            throw new IllegalArgumentException("Empty list of fields.");
        }

        return resolveKeyMap(input);
    }

    private static Map<MatchKey, List<String>> resolveKeyMap(MatchInput input) {
        Map<MatchKey, List<String>> keyMap = input.getKeyMap();
        if (!input.isSkipKeyResolution()) {
            keyMap = MatchKeyUtils.resolveKeyMap(input.getFields());
            if (input.getKeyMap() != null && !input.getKeyMap().keySet().isEmpty()) {
                for (Map.Entry<MatchKey, List<String>> entry : input.getKeyMap().entrySet()) {
                    log.debug("Overwriting key map entry " + JsonUtils.serialize(entry));
                    keyMap.put(entry.getKey(), entry.getValue());
                }
            }
        } else if (keyMap == null || keyMap.isEmpty()) {
            throw new IllegalArgumentException("Have to provide a key map, when skipping automatic key resolution.");
        }

        for (List<String> fields : keyMap.values()) {
            if (fields != null && !fields.isEmpty()) {
                for (String field : fields) {
                    if (!input.getFields().contains(field)) {
                        throw new IllegalArgumentException(
                                "Cannot find target field " + field + " in claimed field list.");
                    }
                }
            }
        }

        return keyMap;
    }

    private static void validateColumnSelection(MatchInput input) {
        if (Predefined.ID.equals(input.getPredefinedSelection())) {
            return;
        }
        if (input.getPredefinedSelection() == null && input.getCustomSelection() == null
                && input.getUnionSelection() == null) {
            throw new IllegalArgumentException("Must specify predefined, custom, or union column selection.");
        } else if (input.getUnionSelection() != null) {
            validateUnionSelection(input.getUnionSelection());
        } else if (input.getCustomSelection() == null) {
            validatePredefinedSelection(input.getPredefinedSelection());
        }
    }

    private static void validateUnionSelection(UnionSelection unionSelection) {
        if (unionSelection.getPredefinedSelections().isEmpty()
                && (unionSelection.getCustomSelection() == null || !unionSelection.getCustomSelection().isEmpty())) {
            throw new IllegalArgumentException(
                    "Must provide predefined or custom column selections in a union selection.");
        } else {
            for (Predefined predefined : unionSelection.getPredefinedSelections().keySet()) {
                validatePredefinedSelection(predefined);
            }
        }
    }

    private static void validatePredefinedSelection(Predefined selection) {
        if (!Predefined.supportedSelections.contains(selection)) {
            throw new UnsupportedOperationException("Only Predefined selection "
                    + Predefined.supportedSelections + " are supported at this time.");
        }
    }

    private static void validateKeys(Set<MatchKey> keySet) {
        if (!keySet.contains(MatchKey.Domain) && !keySet.contains(MatchKey.Name) && !keySet.contains(MatchKey.LatticeAccountID)) {
            throw new IllegalArgumentException("Neither domain nor name nor lattice account id is provided.");
        }

        if ((!keySet.contains(MatchKey.Domain) && !keySet.contains(MatchKey.LatticeAccountID)) && keySet.contains(MatchKey.Name)
                && (!keySet.contains(MatchKey.Country) || !keySet.contains(MatchKey.State))) {
            throw new IllegalArgumentException("Name location based match must has country and state.");
        }
    }

    private static List<String> validateInputAvroAndGetFieldNames(InputBuffer buffer, Configuration yarnConfiguration) {
        AvroInputBuffer avroInputBuffer = (AvroInputBuffer) buffer;
        String avroDir = avroInputBuffer.getAvroDir();
        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, avroDir)) {
                throw new IllegalStateException("Cannot find avro dir " + avroDir);
            }

            Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, MatchUtils.toAvroGlobs(avroDir));
            if (!iterator.hasNext()) {
                throw new IllegalArgumentException("0 rows in input avro(s)");
            }

            Schema schema = extractSchema(avroDir, yarnConfiguration);
            List<String> fieldNames = new ArrayList<>();
            for (Schema.Field field : schema.getFields()) {
                fieldNames.add(field.name());
            }
            return fieldNames;
        } catch (Exception e) {
            throw new RuntimeException("Cannot validate the avro input buffer: " + JsonUtils.serialize(buffer), e);
        }
    }

    private static Schema extractSchema(String avroDir, Configuration yarnConfiguration) throws Exception {
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, MatchUtils.toAvroGlobs(avroDir));
        if (files.size() > 0) {
            String avroPath = files.get(0);
            return AvroUtils.getSchema(yarnConfiguration, new org.apache.hadoop.fs.Path(avroPath));
        } else {
            throw new IllegalStateException("No avro file found at " + avroDir);
        }
    }

}
