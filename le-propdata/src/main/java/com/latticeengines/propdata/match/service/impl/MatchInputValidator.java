package com.latticeengines.propdata.match.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyUtils;

class MatchInputValidator {
    private static Log log = LogFactory.getLog(MatchInputValidator.class);

    public static void validateRealTimeInput(MatchInput input, int maxRealTimeInput) {
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
                log.info("Overwriting key map entry " + JsonUtils.serialize(entry));
                keyMap.put(entry.getKey(), entry.getValue());
            }
        }
        input.setKeyMap(keyMap);

        for (List<String> fields : input.getKeyMap().values()) {
            if (fields != null && !fields.isEmpty()) {
                for (String field: fields) {
                    if (!input.getFields().contains(field)) {
                        throw new IllegalArgumentException("Cannot find target field " + field + " in claimed field list.");
                    }
                }
            }
        }

        validateKeys(input.getKeyMap().keySet());

        if (input.getData() == null || input.getData().isEmpty()) {
            throw new IllegalArgumentException("Empty input data.");
        }

        if (input.getData().size() > maxRealTimeInput) {
            throw new IllegalArgumentException("Too many input data, maximum rows = " + maxRealTimeInput);
        }
    }

    private static void validateColumnSelection(MatchInput input) {
        if (input.getPredefinedSelection() == null && input.getCustomSelection() == null) {
            throw new IllegalArgumentException("Must specify predefined or custom column selection.");
        }

        if (!ColumnSelection.Predefined.Model.equals(input.getPredefinedSelection()) &&
                !ColumnSelection.Predefined.DerivedColumns.equals(input.getPredefinedSelection())) {
            throw new UnsupportedOperationException(
                    "Only Predefined selection " + ColumnSelection.Predefined.Model
                            + " and " + ColumnSelection.Predefined.DerivedColumns + " is supported at this time.");
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

}
