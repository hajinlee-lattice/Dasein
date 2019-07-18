package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.IOBufferType;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class MatchInputValidator {
    private static Logger log = LoggerFactory.getLogger(MatchInputValidator.class);

    public static void validateRealTimeInput(MatchInput input, int maxRealTimeInput) {
        validateRealTimeInput(input, maxRealTimeInput, null);
    }

    public static void validateRealTimeInput(MatchInput input, int maxRealTimeInput, DecisionGraph decisionGraph) {
        validateMatchInput(input, decisionGraph);
        validateInputData(input, maxRealTimeInput);
    }

    public static void validateBulkInput(MatchInput input, Configuration yarnConfiguration) {
        validateBulkInput(input, yarnConfiguration, null);
    }

    public static void validateBulkInput(MatchInput input, Configuration yarnConfiguration,
            DecisionGraph decisionGraph) {
        if (input.getInputBuffer() == null) {
            throw new IllegalArgumentException("Bulk input must have an IO buffer.");
        }

        if (input.getOutputBufferType() == null) {
            log.info("Output buffer type is unset, using the input buffer type "
                    + input.getInputBuffer().getBufferType() + " as default.");
            input.setOutputBufferType(input.getInputBuffer().getBufferType());
        }

        if (IOBufferType.SQL.equals(input.getOutputBufferType())) {
            throw new UnsupportedOperationException("Only the IOBufferType [AVRO] is supported.");
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

        validateMatchInput(input, decisionGraph);
    }

    /*
     * validation for match input base on operational mode
     */
    private static void validateMatchInput(MatchInput input, DecisionGraph decisionGraph) {
        if (input.getTenant() == null) {
            throw new IllegalArgumentException("Must provide tenant to run a match.");
        }
        if (input.getTenant().getId() == null) {
            throw new IllegalArgumentException("Must provide tenant identifier to run a match.");
        }

        if (CollectionUtils.isEmpty(input.getFields())) {
            throw new IllegalArgumentException("Empty list of fields.");
        }

        // Perform a different set of validation operations for Entity Match
        // case.
        if (OperationalMode.isEntityMatch(input.getOperationalMode())) {
            // TODO add validation for entity match attr lookup
            validateEntityMatch(input, decisionGraph);
        } else {
            input.setKeyMap(validateNonEntityMatch(input));
            if (MatchUtils.isValidForRTSBasedMatch(input.getDataCloudVersion())) {
                validateRTSMatchKeys(input.getKeyMap().keySet());
            } else {
                validateLDCAccountMatchKeys(input.getKeyMap().keySet());
            }
        }
    }

    private static void validateEntityMatch(MatchInput input, DecisionGraph decisionGraph) {
        // Validate flags for entity match are set properly
        validateEntityMatchFlags(input);

        // Validate naming conflict in match input fields
        validateEntityMatchInputFields(input);

        // Verify that column selection is set appropriately for Entity Match.
        validateEntityMatchColumnSelection(input);

        // Verify whether decision graph and entity are matched in MatchInput.
        validateEntityMatchDecisionGraph(input, decisionGraph);

        // For now, Entity Match does not support automatic key resolution.
        if (!input.isSkipKeyResolution()) {
            log.warn("isSkipKeyResolution must be set true for Entity Match: "
                    + "Automatic match key resolution not yet supported.");
        }

        // instantiate empty key maps if necessary
        if (input.getEntityKeyMaps() == null) {
            input.setEntityKeyMaps(new HashMap<>());
        }

        // TODO: Add other code to process other EntityKeyMaps besides the Account
        // EntityKeyMap.
        for (Map.Entry<String, EntityKeyMap> entry : input.getEntityKeyMaps().entrySet()) {
            EntityKeyMap entityKeyMap = entry.getValue();
            if (entityKeyMap == null) {
                // instantiate an empty key map
                entityKeyMap = new EntityKeyMap();
                entry.setValue(entityKeyMap);
            }
            entityKeyMap.setKeyMap(resolveKeyMap(entityKeyMap.getKeyMap(), input.getFields(), true, true));
        }

        if (input.getEntityKeyMaps().containsKey(BusinessEntity.Account.name())) {
            EntityKeyMap entityKeyMap = input.getEntityKeyMaps().get(BusinessEntity.Account.name());
            Map<MatchKey, List<String>> keyMap = entityKeyMap.getKeyMap();

            validateAccountMatchKeys(keyMap, input.isFetchOnly());
        }
    }

    private static void validateEntityMatchFlags(MatchInput input) {
        if (input.isAllocateId() && input.isFetchOnly()) {
            throw new IllegalArgumentException("AllocateID mode and FetchOnly mode cannot be set at same time");
        }
    }

    /**
     * TODO: For now, only validate Non-FetchOnly mode to avoid attribute conflict
     * in match result (EntityId).
     *
     * FetchOnly mode validation will be more complicate because it could fetch any
     * attribute from Seed table. Whether we should fail any attribute name conflict
     * or overwrite existing attribute or do some attribute rename (hard to read
     * renamed attributes as renaming logic is not exposed to outside of matcher),
     * needs more thoughts
     *
     * @param input
     */
    private static void validateEntityMatchInputFields(MatchInput input) {
        if (input.isFetchOnly()) {
            return;
        }
        List<String> reservedFields = input.getFields().stream() //
                .filter(MatchKeyUtils::isEntityReservedField) //
                .collect(Collectors.toList());
        if (!reservedFields.isEmpty()) {
            throw new IllegalArgumentException(
                    "Reserved fields are not allowed to show up in Non-FetchOnly mode match input fields: "
                            + String.join(",", reservedFields));
        }
    }

    @VisibleForTesting
    static void validateEntityMatchColumnSelection(MatchInput input) {
        // Only predefined column selection type is permitted for Entity Match.
        if (input.getPredefinedSelection() == null) {
            throw new IllegalArgumentException("Entity Match must have predefined column selection set.");
        }

        // Check that custom and union column selection are not set for Entity Match.
        if (input.getCustomSelection() != null) {
            throw new IllegalArgumentException("Entity Match cannot have custom column selection set.");
        }
        if (input.getUnionSelection() != null) {
            throw new IllegalArgumentException("Entity Match cannot have union column selection set.");
        }

        // For Entity Match Allocated ID mode, predefined column selection must be "ID".
        // Otherwise, the predefined
        // column selection must be a valid value.
        if (input.isAllocateId()) {
            if (!Predefined.ID.equals(input.getPredefinedSelection())) {
                throw new UnsupportedOperationException(
                        "Entity Match Allocate ID mode only supports predefined column selection set to \"ID\".");
            }
        } else {
            validatePredefinedSelection(input.getPredefinedSelection(), OperationalMode.ENTITY_MATCH);
        }
    }

    private static void validateEntityMatchDecisionGraph(MatchInput input, DecisionGraph decisionGraph) {
        if (StringUtils.isBlank(input.getDecisionGraph())) {
            return; // Use default decision graph
        }
        if (StringUtils.isNotBlank(input.getDecisionGraph()) && decisionGraph == null) {
            throw new IllegalArgumentException("Cannot find decision graph with name " + input.getDecisionGraph());
        }
        if (StringUtils.isBlank(input.getDecisionGraph()) && StringUtils.isBlank(input.getTargetEntity())) {
            throw new IllegalArgumentException(
                    "Please provide either decision graph or target entity for entity match");
        }
        if (StringUtils.isNotBlank(input.getDecisionGraph()) && StringUtils.isNotBlank(input.getTargetEntity())
                && !input.getTargetEntity().equals(decisionGraph.getEntity())) {
            throw new IllegalArgumentException(String.format(
                    "Decision graph %s and target entity %s are not matched. Target entity for decision graph %s is %s",
                    input.getDecisionGraph(), input.getTargetEntity(), input.getDecisionGraph(),
                    decisionGraph.getEntity()));
        }
    }

    private static Map<MatchKey, List<String>> validateNonEntityMatch(MatchInput input) {
        validateNonEntityMatchColumnSelection(input);

        if (MapUtils.isNotEmpty(input.getKeyMap()) && input.getKeyMap().containsKey(MatchKey.LookupId)) {
            if (input.getKeyMap().get(MatchKey.LookupId).size() != 1) {
                throw new IllegalArgumentException(
                        "Can only specify one field as lookup id: " + input.getKeyMap().get(MatchKey.LookupId));
            }
        }

        return resolveKeyMap(input.getKeyMap(), input.getFields(), input.isSkipKeyResolution(), false);
    }

    private static Map<MatchKey, List<String>> resolveKeyMap(Map<MatchKey, List<String>> keyMap,
            List<String> inputFields, boolean isSkipKeyResolution, boolean allowEmptyKeyMap) {
        Map<MatchKey, List<String>> newKeyMap = keyMap;
        // TODO: Automatic key resolution is not allowed for the Entity Match case. This
        // code would have to be changed to work because it is not set up to handle
        // multiple key maps, one for each entity, and would currently populate each
        // entity's key map with all input fields.
        if (!isSkipKeyResolution) {
            newKeyMap = MatchKeyUtils.resolveKeyMap(inputFields);
            if (MapUtils.isNotEmpty(keyMap)) {
                for (Map.Entry<MatchKey, List<String>> entry : keyMap.entrySet()) {
                    log.debug("Overwriting key map entry " + JsonUtils.serialize(entry));
                    newKeyMap.put(entry.getKey(), entry.getValue());
                }
            }
        } else if (MapUtils.isEmpty(newKeyMap)) {
            if (!allowEmptyKeyMap) {
                throw new IllegalArgumentException(
                        "Have to provide a key map, when skipping automatic key resolution.");
            } else {
                // allow user to not map any fields, instantiating an empty map
                newKeyMap = new HashMap<>();
            }
        }

        /*-
         * Validate the MatchKeys by checking:
         * a) That all keys are non-null.
         * b) That all values are either:
         *   i) null
         *   ii) empty lists
         *   iii) lists composed of non-null and non-empty strings.
         * Note: Using a key with a null or empty list value is allowed as a way to indicate that this key should
         * be ignored in match as also a way to auto-resolution from populating that key.
         */
        for (Map.Entry<MatchKey, List<String>> entry : newKeyMap.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("MatchKey key must be non-null.");
            } else if (CollectionUtils.isNotEmpty(entry.getValue())) {
                for (String elem : entry.getValue()) {
                    if (StringUtils.isBlank(elem)) {
                        throw new IllegalArgumentException(
                                "MatchKey value list elements must be non-null and non-empty.");
                    } else if (!inputFields.contains(elem)) {
                        throw new IllegalArgumentException(
                                "Cannot find MatchKey value element " + elem + " in claimed field list."
                                        + "\n\nInput Fields contains: " + String.join(" ", inputFields) + "\nMatchKey: "
                                        + entry.getKey() + " values: " + String.join(" ", entry.getValue()));
                    }
                }
            }
        }

        return newKeyMap;
    }

    private static void validateNonEntityMatchColumnSelection(MatchInput input) {
        // TODO(dzheng): This first return clause seems to assume that if Predefined
        // Column Selection is set to "ID", there is no Union Column Selection or Custom
        // Column Selection added, since they do not get validated.
        // Is this always true?
        if (Predefined.ID.equals(input.getPredefinedSelection())) {
            return;
        }
        if (input.getPredefinedSelection() == null && input.getCustomSelection() == null
                && input.getUnionSelection() == null) {
            throw new IllegalArgumentException("Must specify predefined, custom, or union column selection.");
        } else if (input.getUnionSelection() != null) {
            validateUnionSelection(input.getUnionSelection(), input.getOperationalMode());
        } else if (input.getCustomSelection() == null) {
            validatePredefinedSelection(input.getPredefinedSelection(), input.getOperationalMode());
        }
    }

    private static void validateUnionSelection(UnionSelection unionSelection, OperationalMode mode) {
        if (unionSelection.getPredefinedSelections().isEmpty()
                && (unionSelection.getCustomSelection() == null || !unionSelection.getCustomSelection().isEmpty())) {
            throw new IllegalArgumentException(
                    "Must provide predefined or custom column selections in a union selection.");
        } else {
            for (Predefined predefined : unionSelection.getPredefinedSelections().keySet()) {
                validatePredefinedSelection(predefined, mode);
            }
        }
    }

    private static void validatePredefinedSelection(Predefined selection, OperationalMode mode) {
        if (!OperationalMode.ENTITY_MATCH.equals(mode) && !Predefined.supportedSelections.contains(selection)) {
            throw new UnsupportedOperationException(
                    "Only Predefined selection " + Predefined.supportedSelections + " are supported at this time.");
        }
        if (OperationalMode.ENTITY_MATCH.equals(mode) && !Predefined.entitySupportedSelections.contains(selection)) {
            throw new UnsupportedOperationException("Only Predefined selection " + Predefined.entitySupportedSelections
                    + " are supported for entity match at this time.");
        }
    }

    /**
     * For 2.0 AccountMaster based LDC matcher:
     *
     * We require one of following MatchKey provided:
     *
     * LatticeAccountId (Fetch-Only), Domain, DUNS, Name, LookupId (CDL Lookup)
     *
     * @param keySet
     */
    private static void validateLDCAccountMatchKeys(Set<MatchKey> keySet) {
        if (!keySet.contains(MatchKey.DUNS) && !keySet.contains(MatchKey.Domain) && !keySet.contains(MatchKey.Name)
                && !keySet.contains(MatchKey.LatticeAccountID) && !keySet.contains(MatchKey.LookupId)) {
            throw new IllegalArgumentException(
                    "Neither domain nor name nor duns nor lattice account id not cdl id is provided for LDC 2.0 matcher");
        }
    }

    /**
     * For 1.0 RTS matcher whose target is sql table DerivedColumnsCache:
     *
     * For exact sql lookup in DerivedColumnsCache (no fuzzy match), we require one
     * of following MatchKey (combination) provided:
     *
     * LatticeAccountId (Fetch-Only), Domain, [Name, Country, State]
     *
     * @param keySet
     */
    private static void validateRTSMatchKeys(Set<MatchKey> keySet) {
        if (!keySet.contains(MatchKey.Domain) && !keySet.contains(MatchKey.Name)
                && !keySet.contains(MatchKey.LatticeAccountID)) {
            throw new IllegalArgumentException(
                    "Neither domain nor name nor lattice account id is provided for RTS 1.0 matcher");
        }

        if ((!keySet.contains(MatchKey.Domain) && !keySet.contains(MatchKey.LatticeAccountID))
                && keySet.contains(MatchKey.Name)
                && (!keySet.contains(MatchKey.Country) || !keySet.contains(MatchKey.State))) {
            throw new IllegalArgumentException(
                    "Name location based match must has country and state for RTS 1.0 matcher");
        }
    }

    /**
     * Since user is allowed to not map any match field, restriction for match keys
     * is relaxed. Only check for fetch only mode for now.
     *
     * Compare to validateLDCAccountMatchKeys():
     *
     * Don't allow setting MatchKey without mapping any fields. In
     * validateLDCAccountMatchKeys(), if setting match key without mapping field,
     * matcher interprets it as not using this match key. Feel this tolerance makes
     * match key config confusing. If don't use, then don't set. Don't make change
     * to validateLDCAccountMatchKeys() because it could break some existing
     * external services
     *
     * @param keyMap
     * @param fetchOnly
     */
    private static void validateAccountMatchKeys(Map<MatchKey, List<String>> keyMap, boolean fetchOnly) {
        if (fetchOnly) {
            if (!isKeyMappedToField(keyMap, MatchKey.EntityId)) {
                throw new IllegalArgumentException(
                        "For fetch-only mode Account match, must provide EntityId match key");
            }
        }
    }

    private static boolean isKeyMappedToField(Map<MatchKey, List<String>> keyMap, MatchKey key) {
        // Mapped fields for key have verified to be not blank in
        // resolveKeyMap()
        return CollectionUtils.isNotEmpty(keyMap.get(key));
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
                throw new IllegalArgumentException("0 rows in input avro(s).");
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

    private static void validateInputData(MatchInput input, int maxRealTimeInput) {
        if (CollectionUtils.isEmpty(input.getData())) {
            throw new IllegalArgumentException("Empty input data.");
        }

        if (input.getData().size() > maxRealTimeInput) {
            throw new IllegalArgumentException("Too many input data, maximum rows = " + maxRealTimeInput + ".");
        }

        // Check that each sub-list in input data is the same length as the number of
        // fields.
        for (List<Object> sublist : input.getData()) {
            if (sublist.size() > input.getFields().size()) {
                throw new IllegalArgumentException(
                        "Input data length must be less than or equal to input fields length.");
            }
        }
    }

}
