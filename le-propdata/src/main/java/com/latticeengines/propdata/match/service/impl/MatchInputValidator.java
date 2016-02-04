package com.latticeengines.propdata.match.service.impl;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyUtils;

class MatchInputValidator {
    private static Log log = LogFactory.getLog(MatchInputValidator.class);

    static void validate(MatchInput input, Integer maxRealTimeInput) {
        if (input.getTenant() == null) {
            throw new IllegalArgumentException("Must provide tenant to run a match.");
        }

        if (input.getMatchEngine() == null) {
            throw new IllegalArgumentException("Must specify match engine.");
        }

        validateColumnSelection(input);

        if (MatchInput.MatchEngine.RealTime.equals(input.getMatchEngine())) {
            validateRealTimeInput(input, maxRealTimeInput);
        } else {
            throw new UnsupportedOperationException(
                    "Match engine " + MatchInput.MatchEngine.Bulk + " is not supported.");
        }
    }

    private static void validateColumnSelection(MatchInput input) {
        if (input.getPredefinedSelection() == null && input.getCustomSelection() == null) {
            throw new IllegalArgumentException("Must specify predefined or custom column selection.");
        }

        if (!ColumnSelection.Predefined.Model.equals(input.getPredefinedSelection())) {
            throw new UnsupportedOperationException(
                    "Only Predefined selection " + ColumnSelection.Predefined.Model + " is supported at this time.");
        }

    }

    private static void validateRealTimeInput(MatchInput input, int maxRealTimeInput) {
        if (input.getFields() == null || input.getFields().isEmpty()) {
            throw new IllegalArgumentException("Empty list of fields.");
        }

        if (input.getKeyMap() == null || input.getKeyMap().keySet().isEmpty()) {
            log.info("Did not find KeyMap in the input. Try to resolve the map from field list.");
            input.setKeyMap(MatchKeyUtils.resolveKeyMap(input.getFields()));
        }

        for (String field : input.getKeyMap().values()) {
            if (!input.getFields().contains(field)) {
                throw new IllegalArgumentException("Cannot find target field " + field + " in claimed field list.");
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

    private static void validateKeys(Set<MatchKey> keySet) {
        if (!keySet.contains(MatchKey.Domain) && !keySet.contains(MatchKey.Name)) {
            throw new IllegalArgumentException("Neither domain nor name is provided.");
        }

        if (!keySet.contains(MatchKey.Domain)) {
            throw new UnsupportedOperationException("Only domain based match is supported for now.");
        }
    }

}
