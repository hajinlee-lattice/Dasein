package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.propdata.match.IOBufferType;
import com.latticeengines.domain.exposed.propdata.match.InputBuffer;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyUtils;

class MatchInputValidator {
    private static Log log = LogFactory.getLog(MatchInputValidator.class);

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
                throw new IllegalArgumentException("Unknown type of input buffer " + input.getInputBuffer().getBufferType());
        }
        input.setFields(inputFields);
        input.setNumRows(input.getInputBuffer().getNumRows().intValue());

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

        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(input.getFields());
        if (input.getKeyMap() != null && !input.getKeyMap().keySet().isEmpty()) {
            for (Map.Entry<MatchKey, List<String>> entry: input.getKeyMap().entrySet()) {
                log.debug("Overwriting key map entry " + JsonUtils.serialize(entry));
                keyMap.put(entry.getKey(), entry.getValue());
            }
        }

        for (List<String> fields : keyMap.values()) {
            if (fields != null && !fields.isEmpty()) {
                for (String field: fields) {
                    if (!input.getFields().contains(field)) {
                        throw new IllegalArgumentException("Cannot find target field " + field + " in claimed field list.");
                    }
                }
            }
        }

        return keyMap;
    }

    private static void validateColumnSelection(MatchInput input) {
        if (input.getPredefinedSelection() == null && input.getCustomSelection() == null) {
            throw new IllegalArgumentException("Must specify predefined or custom column selection.");
        }

        if (!ColumnSelection.Predefined.Model.equals(input.getPredefinedSelection()) &&
                !ColumnSelection.Predefined.DerivedColumns.equals(input.getPredefinedSelection())) {
            throw new UnsupportedOperationException(
                    "Only Predefined selection " + ColumnSelection.Predefined.Model
                            + " and " + ColumnSelection.Predefined.DerivedColumns + " are supported at this time.");
        }

    }

    private static void validateKeys(Set<MatchKey> keySet) {
        if (!keySet.contains(MatchKey.Domain) && !keySet.contains(MatchKey.Name)) {
            throw new IllegalArgumentException("Neither domain nor name is provided.");
        }

        if (!keySet.contains(MatchKey.Domain) && keySet.contains(MatchKey.Name)
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

            Long count;
            try {
                count = AvroUtils.count(yarnConfiguration, avroDir + "/*.avro");
                log.info("Find " + count + " records in the avro buffer dir " + avroDir);
            } catch (Exception e) {
                throw new RuntimeException("Failed to count input data", e);
            }

            if (count == 0L) {
                throw new IllegalArgumentException("0 rows in input avro(s)");
            }

            buffer.setNumRows(count);

            Schema schema = extractSchema(avroDir, yarnConfiguration);
            List<String> fieldNames = new ArrayList<>();
            for (Schema.Field field: schema.getFields()) {
                fieldNames.add(field.name());
            }
            return fieldNames;
        } catch (Exception e) {
            throw new RuntimeException("Cannot validate the avro input buffer: " + JsonUtils.serialize(buffer), e);
        }
    }

    private static Schema extractSchema(String avroDir, Configuration yarnConfiguration) throws Exception {
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro");
        if (files.size() > 0) {
            String avroPath = files.get(0);
            return AvroUtils.getSchema(yarnConfiguration, new org.apache.hadoop.fs.Path(avroPath));
        } else {
            throw new IllegalStateException("No avro file found at " + avroDir);
        }
    }

}
