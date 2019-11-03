package com.latticeengines.datacloud.match.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchHistory;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class MatchHistoryUtils {
    private static final Logger log = LoggerFactory.getLogger(MatchHistoryUtils.class);

    private enum PreferredIdOutcome {
        NO_ID,
        INVALID_ID,
        ID_NOT_USED_MERGED_TO_EXISTING,
        ID_NOT_USED_ALREADY_TAKEN,
        ID_USED
    }

    // Copy raw and parsed preferred entity ID maps to entity match history.  Then, create a map of entity to
    // resulting outcome of the preferred ID.
    public static void processPreferredIds(InternalOutputRecord record, MatchTraveler traveler,
                                           EntityMatchHistory history) {
        history.setRawPreferredEntityIds(record.getOrigPreferredEntityIds());
        logMap("Raw Preferred Entity Ids", history.getRawPreferredEntityIds());
        history.setStandardisedPreferredEntityIds(record.getParsedPreferredEntityIds());
        logMap("Standardised Preferred Entity Ids", history.getStandardisedPreferredEntityIds());

        Map<String, String> outcomesMap = new HashMap<>();
        // Iterate through the entity IDs map, whose keys contain all the entities involved in this record.
        if (MapUtils.isEmpty(traveler.getEntityIds())) {
            log.warn("Entity ID map is empty");
            history.setPreferredEntityIdOutcomes(outcomesMap);
            return;
        }

        for (Map.Entry<String, String> entry : traveler.getEntityIds().entrySet()) {
        //// Iterate through raw preferred entity ID map and determine the preferred ID resulting state for each entity.
        //for (Map.Entry<String, String> entry : history.getRawPreferredEntityIds().entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                log.error("Found null or empty key in EntityIds map");
                continue;
            } else if (BusinessEntity.LatticeAccount.name().equals(entry.getKey())) {
                // Skip the LatticeAccount entity.
                continue;
            }

            // Now determine the outcome of the attempt to use the preferred ID.
            //if (StringUtils.isBlank(entry.getValue())) {
            if (MapUtils.isEmpty(history.getRawPreferredEntityIds()) ||
                    StringUtils.isBlank(history.getRawPreferredEntityIds().get(entry.getKey()))) {
                // Case 1: No preferred ID was provided by the user.
                outcomesMap.put(entry.getKey(), PreferredIdOutcome.NO_ID.name());
            } else if (MapUtils.isEmpty(history.getStandardisedPreferredEntityIds()) ||
                    StringUtils.isBlank(history.getStandardisedPreferredEntityIds().get(entry.getKey()))) {
                // Case 2: Preferred ID provided was invalid and could not be parsed.
                outcomesMap.put(entry.getKey(), PreferredIdOutcome.INVALID_ID.name());
            } else {
                String standardisedPreferredId = history.getStandardisedPreferredEntityIds().get(entry.getKey());
                String entityId = entry.getValue();
                if (StringUtils.isBlank(entityId)) {
                    log.error("Found null or empty entityId");
                    continue;
                }
                if (!StringUtils.equals(standardisedPreferredId, entityId)) {
                    if (MapUtils.isEmpty(traveler.getNewEntityIds()) ||
                            StringUtils.isBlank(traveler.getNewEntityIds().get(entry.getKey()))) {
                        // Case 3: Preferred ID was valid but not used (did not match the entity ID).  The entity does
                        // not have a newly allocated entity ID, indicated that this row was matched to an existing
                        // entity and the old entity ID was used.
                        outcomesMap.put(entry.getKey(), PreferredIdOutcome.ID_NOT_USED_MERGED_TO_EXISTING.name());
                    } else {
                        // DEBUG
                        String newEntityId = traveler.getNewEntityIds().get(entry.getKey());
                        if (StringUtils.equals(standardisedPreferredId, newEntityId)) {
                            throw new RuntimeException(String.format(
                                    "Preferred ID %s should not equal new Entity ID %s if not equal to Entity ID %s",
                                    standardisedPreferredId, newEntityId, entityId));
                        } else if (!StringUtils.equals(entityId, newEntityId)) {
                            throw new RuntimeException(String.format(
                                    "Entity ID %s and New Entity ID %s should be equal", entityId, newEntityId));
                        }
                        // Case 4: Preferred ID was valid but not used (did not match the entity ID).  There is a
                        // newly allocated entity ID, which means that the preferred ID was not used because it was
                        // already taken.
                        outcomesMap.put(entry.getKey(), PreferredIdOutcome.ID_NOT_USED_ALREADY_TAKEN.name());
                    }
                } else {
                    // Case 5: Preferred ID provided was used as the entity ID.
                    outcomesMap.put(entry.getKey(), PreferredIdOutcome.ID_USED.name());
                }
            }
        }
        history.setPreferredEntityIdOutcomes(outcomesMap);

        logMap("Preferred Entity Id Outcomes", history.getPreferredEntityIdOutcomes());
    }


    private static void logMap(String mapName, Map<String, String> map) {
        StringBuilder builder = new StringBuilder();
        builder.append("Map: ").append(mapName).append("\n");
        if (MapUtils.isEmpty(map)) {
            builder.append("    <EMPTY>");
        } else {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                builder.append("    ").append(entry.getKey()).append(":  ").append(entry.getValue()).append("\n");
            }
        }
        log.debug(builder.toString());
    }
}
