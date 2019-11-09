package com.latticeengines.datacloud.match.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchType;
import com.latticeengines.domain.exposed.datacloud.match.LdcMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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

    // Extract EntityMatchType Enum describing match.
    public static EntityMatchType extractEntityMatchType(String entity, MatchKeyTuple tuple,
                                                         List<String> lookupResultList, String inputDuns) {
        EntityMatchType entityMatchType;
        if (tuple == null) {
            entityMatchType = EntityMatchType.NO_MATCH;
        } else {
            entityMatchType = EntityMatchType.UNKNOWN;

            boolean hasAccountId = false;
            if (CollectionUtils.isNotEmpty(tuple.getSystemIds())) {
                if (lookupResultList.size() != tuple.getSystemIds().size()) {
                    log.error("EntityMatchLookupResults results and MatchKeyTuple SystemIds sizes don't match");
                    log.error("EntityMatchLookupResults: " + lookupResultList.toString());
                    log.error("MatchKeyTuple SystemIds: " + tuple.getSystemIds().toString());
                    return null;
                }
                int i = 0;
                for (Pair<String, String> systemId : tuple.getSystemIds()) {
                    if (InterfaceName.CustomerAccountId.name().equals(systemId.getKey())
                            && StringUtils.isNotBlank(systemId.getValue())
                            && StringUtils.isNotBlank(lookupResultList.get(i))
                            && BusinessEntity.Account.name().equals(entity)) {
                        entityMatchType = EntityMatchType.ACCOUNTID;
                        // log.debug("MatchKeyTuple contains CustomerAccountId: " +
                        // systemId.getValue());
                        break;
                    } else if (InterfaceName.CustomerContactId.name().equals(systemId.getKey())
                            && StringUtils.isNotBlank(systemId.getValue())
                            && StringUtils.isNotBlank(lookupResultList.get(i))
                            && BusinessEntity.Contact.name().equals(entity)) {
                        entityMatchType = EntityMatchType.CONTACTID;
                        // log.debug("MatchKeyTuple contains CustomerContactId: " +
                        // systemId.getValue());
                        break;
                    } else if (InterfaceName.AccountId.name().equals(systemId.getKey())
                            && StringUtils.isNotBlank(systemId.getValue())
                            && BusinessEntity.Contact.name().equals(entity)) {
                        // log.debug("MatchKeyTuple contains AccountId: " + systemId.getValue());
                        hasAccountId = true;
                    } else if (StringUtils.isNotBlank(systemId.getKey()) && StringUtils.isNotBlank(systemId.getValue())
                            && StringUtils.isNotBlank(lookupResultList.get(i))) {
                        entityMatchType = EntityMatchType.SYSTEMID;
                        // log.debug("MatchKeyTuple contains SystemId: " + systemId.getKey() + " with
                        // value: "
                        // + systemId.getValue());
                        break;
                    }
                    i++;
                }
            }

            if (entityMatchType == EntityMatchType.UNKNOWN) {
                if (tuple.hasDomain()) {
                    entityMatchType = EntityMatchType.DOMAIN_COUNTRY;
                } else if (tuple.hasDuns()) {
                    // Set the match type to DUNS if the input has a DUNS value and it equals the DUNS value
                    // the Entity Match system stored in the MatchKeyTuple, which is the final DUNS result.
                    if (StringUtils.isNotBlank(inputDuns) && inputDuns.equals(tuple.getDuns())) {
                        entityMatchType = EntityMatchType.DUNS;
                    } else {
                        entityMatchType = EntityMatchType.LDC_MATCH;
                    }
                } else if (tuple.hasEmail()) {
                    if (hasAccountId) {
                        entityMatchType = EntityMatchType.EMAIL_ACCOUNTID;
                    } else {
                        entityMatchType = EntityMatchType.EMAIL;
                    }
                } else if (tuple.hasName()) {
                    if (BusinessEntity.Account.name().equals(entity)) {
                        entityMatchType = EntityMatchType.NAME_COUNTRY;
                    } else if (tuple.hasPhoneNumber()) {
                        if (hasAccountId) {
                            entityMatchType = EntityMatchType.NAME_PHONE_ACCOUNTID;
                        } else {
                            entityMatchType = EntityMatchType.NAME_PHONE;
                        }
                    }
                }
            }
        }

        // Log the EntityMatchType and matched Entity MatchKeyTuple.
        log.debug("EntityMatchType is: " + entityMatchType);
        log.debug("MatchedEntityMatchKeyTuple: " + tuple);
        if (BusinessEntity.Account.name().equals(entity)) {
            log.debug("    inputDuns: " + inputDuns + "  tupleDuns: " + (tuple == null ? "null" : tuple.getDuns()));
        } else if (EntityMatchType.LDC_MATCH.equals(entityMatchType)) {
            log.error("Found LDC Match type for entity " + entity + " which should not be possible");
            return null;
        }
        return entityMatchType;
    }

    public static Pair<LdcMatchType, MatchKeyTuple> extractLdcMatchTypeAndTuple(MatchTraveler traveler) {
        // Get matched DUNS from traveler DUNS map for Account Master entry.
        String matchedDuns = null;
        String latticeAccountId = null;
        if (MapUtils.isNotEmpty(traveler.getDunsOriginMap()) &&
                traveler.getDunsOriginMap().containsKey(DataCloudConstants.ACCOUNT_MASTER)) {
            matchedDuns = traveler.getDunsOriginMap().get(DataCloudConstants.ACCOUNT_MASTER);
        }
        if (traveler.getEntityIds().containsKey(BusinessEntity.LatticeAccount.name())) {
            latticeAccountId = traveler.getEntityIds().get(BusinessEntity.LatticeAccount.name());
        }
        log.debug("    AM Matched DUNS: " + matchedDuns);
        log.debug("    Lattice Account ID: " + latticeAccountId);

        LdcMatchType ldcMatchType = LdcMatchType.NO_MATCH;
        MatchKeyTuple ldcMatchedTuple = null;
        // There must be a Lattice Account ID for LDC Match to have succeeded.
        if (!StringUtils.isBlank(latticeAccountId)) {
            if (CollectionUtils.isEmpty(traveler.getEntityLdcMatchTypeToTupleList())) {
                log.error("LDC Match succeeded but entityLdcMatchTypeToTupleList is null or empty");
                return null;
            }

            List<Pair<MatchKeyTuple, List<String>>> ldcMatchLookupResultList = traveler
                    .getEntityMatchLookupResult(BusinessEntity.LatticeAccount.name());
            if (CollectionUtils.isEmpty(ldcMatchLookupResultList)) {
                log.error("LDC Match succeeded but EntityMatchLookupResult for LatticeAccount is null or empty");
                return null;
            }

            if (ldcMatchLookupResultList.size() != traveler.getEntityLdcMatchTypeToTupleList().size()) {
                log.error("EntityMatchLookupResult for {} and EntityLdcMatchTypeToTupleList are not equal length: " +
                                "{} vs {}", BusinessEntity.LatticeAccount.name(), ldcMatchLookupResultList.size(),
                        traveler.getEntityLdcMatchTypeToTupleList().size());
                MatchHistoryUtils.generateEntityMatchHistoryDebugLogs(traveler);
                return null;
            }

            // Iterate through the lists of LDC Match Lookup Results and LDC Match Type / MatchKeyTuple pairs, to find
            // the the first successful result. Then record the corresponding LDC Match Type and MatchKeyTuple of
            // that result.
            int i;
            boolean foundResult = false;
            for (i = 0; i < ldcMatchLookupResultList.size() && !foundResult; i++) {
                if (CollectionUtils.isEmpty(ldcMatchLookupResultList.get(i).getValue())) {
                    log.error("EntityMatchLookupResult for " + BusinessEntity.LatticeAccount.name()
                            + " has list entry composed of a Pair with null value and key MatchKeyTuple: "
                            + ldcMatchLookupResultList.get(i).getKey());
                    return null;
                }
                for (String result : ldcMatchLookupResultList.get(i).getValue()) {
                    if (result != null) {
                        ldcMatchType = traveler.getEntityLdcMatchTypeToTupleList().get(i).getLeft();
                        ldcMatchedTuple = traveler.getEntityLdcMatchTypeToTupleList().get(i).getRight();
                        foundResult = true;
                        break;
                    }
                }
            }

            if (!foundResult) {
                log.error("LDC Match succeeded but Lookup Results has no entry with non-null result");
                return null;
            } else if (ldcMatchType == null) {
                log.error("EntityLdcMatchTypeToTupleList entry " + i + " has null LdcMatchType");
                return null;
            } else if (ldcMatchedTuple == null) {
                log.error("EntityLdcMatchTypeToTupleList entry " + i + " has null MatchKeyTuple for type: " + ldcMatchType);
                return null;
            }

            // If the MatchKeyTuple has a DUNS fields and the LdcMatchType is not LDC DUNS or DUNS plus Domain, then the
            // LDC Match has stuck a DUNS value in the matched MatchKeyTuple that wasn't actually part of the input match
            // tuple used for matching. In this case, a copy of the MatchKeyTuple without the DUNS field needs to be
            // created and returned.
            if (ldcMatchedTuple.hasDuns() && !LdcMatchType.DUNS.equals(ldcMatchType)
                    && !LdcMatchType.DUNS_DOMAIN.equals(ldcMatchType)) {
                ldcMatchedTuple = copyLdcTupleNoDuns(ldcMatchedTuple);
            }
        }
        log.debug("LdcMatchType is: " + ldcMatchType);
        log.debug("MatchedLdcMatchKeyTuple: " + ldcMatchedTuple);
        return Pair.of(ldcMatchType, ldcMatchedTuple);
    }

    private static MatchKeyTuple copyLdcTupleNoDuns(MatchKeyTuple tuple) {
        // Don't set System ID since it is not used in LDC match even if it is set.
        return new MatchKeyTuple.Builder().withDomain(tuple.getDomain()).withName(tuple.getName())
                .withCity(tuple.getCity()).withState(tuple.getState()).withCountry(tuple.getCountry())
                .withCountryCode(tuple.getCountryCode()).withZipcode(tuple.getZipcode())
                .withPhoneNumber(tuple.getPhoneNumber()).withEmail(tuple.getEmail()).build();
    }

    public static List<Pair<String, MatchKeyTuple>> extractExistingLookupKeyList(MatchTraveler traveler,
                                                                                 String entity) {
        if (MapUtils.isEmpty(traveler.getEntityExistingLookupEntryMap())) {
            log.debug("EntityExistingLookupEntryMap is null or empty");
            return null;
        } else if (!traveler.getEntityExistingLookupEntryMap().containsKey(entity)
                || MapUtils.isEmpty(traveler.getEntityExistingLookupEntryMap().get(entity))) {
            log.debug("EntityExistingLookupEntryMap for entity " + entity + " is null or empty");
            return new ArrayList<>();
        }
        List<Pair<String, MatchKeyTuple>> existingLookupList = new ArrayList<>();
        for (Map.Entry<EntityMatchType, List<MatchKeyTuple>> typeEntry : traveler.getEntityExistingLookupEntryMap()
                .get(entity).entrySet()) {
            if (CollectionUtils.isEmpty(typeEntry.getValue())) {
                continue;
            }
            for (MatchKeyTuple tuple : typeEntry.getValue()) {
                existingLookupList.add(Pair.of(typeEntry.getKey().name(), tuple));
            }
        }
        return existingLookupList;
    }

    // Assumes traveler.getEntityIds(), traveler.getEntityMatchKeyTuples(), and traveler.getEntityMatchLookupResults()
    // do not return null because they were checked by generateEntityMatchHistory().
    public static void generateEntityMatchHistoryDebugLogs(MatchTraveler traveler) {
        log.debug("------------------------ Entity Match History Extra Debug Logs ------------------------");
        log.debug("EntityIds are: ");
        for (Map.Entry<String, String> entry : traveler.getEntityIds().entrySet()) {
            log.debug("   Entity: " + entry.getKey() + "  EntityId: " + entry.getValue());
        }

        if (MapUtils.isEmpty(traveler.getNewEntityIds())) {
            log.debug("NewEntityIds are: null or empty");
        } else {
            log.debug("NewEntityIds are: ");
            for (Map.Entry<String, String> entry : traveler.getNewEntityIds().entrySet()) {
                log.debug("    Entity: " + entry.getKey() + "  NewEntityId: " + entry.getValue());
            }
        }

        log.debug("EntityMatchKeyTuples is: ");
        for (Map.Entry<String, MatchKeyTuple> entry : traveler.getEntityMatchKeyTuples().entrySet()) {
            log.debug("    Entity: " + entry.getKey());
            log.debug("      MatchKeyTuple: " + entry.getValue().toString());
        }

        log.debug("Iterate through EntityMatchLookupResults:");
        for (Map.Entry<String, List<Pair<MatchKeyTuple, List<String>>>> entry : traveler.getEntityMatchLookupResults()
                .entrySet()) {
            boolean foundResult = false;
            log.debug("    MatchKeyTuple Lookup Results for " + entry.getKey());
            for (Pair<MatchKeyTuple, List<String>> pair : entry.getValue()) {
                if (pair.getKey() == null) {
                    log.debug("        MatchKeyTuple: null");
                } else {
                    log.debug("        MatchKeyTuple: " + pair.getKey().toString());
                }

                String resultList = "";
                for (String result : pair.getValue()) {
                    if (result != null) {
                        if (!foundResult) {
                            foundResult = true;
                            resultList += ">>> " + result + " <<< ";
                        } else {
                            resultList += result + " ";
                        }
                    } else {
                        resultList += "null ";
                    }
                }
                log.debug("        Results: " + resultList);
            }
        }

        if (CollectionUtils.isNotEmpty(traveler.getEntityLdcMatchTypeToTupleList())) {
            log.debug("Iterate through EntityLdcMatchTypeToTupleList:");
            for (Pair<LdcMatchType, MatchKeyTuple> pair : traveler.getEntityLdcMatchTypeToTupleList()) {
                log.debug("    LdcMatchType: " + pair.getLeft() + "  LdcMatchKeyTuple: " + pair.getRight());
            }
        } else {
            log.debug("EntityLdcMatchTypeToTupleList is empty");
        }

        generateDebugExistingMatchKeyLookups(traveler);
        // log.debug("------------------------ END Entity Match History Extra Debug Logs ------------------------");
    }

    private static void generateDebugExistingMatchKeyLookups(MatchTraveler traveler) {
        log.debug("Iterate through existing Entity Match Lookup Keys");
        if (MapUtils.isEmpty(traveler.getEntityExistingLookupEntryMap())) {
            log.debug("   EntityExistingLookupEntryMap is null or empty");
            return;
        }

        for (Map.Entry<String, Map<EntityMatchType, List<MatchKeyTuple>>> entityEntry : traveler
                .getEntityExistingLookupEntryMap().entrySet()) {
            log.debug("  " + entityEntry.getKey() + " - Existing Lookup Entries");
            if (MapUtils.isEmpty(entityEntry.getValue())) {
                log.debug("    <empty>");
                continue;
            }
            for (Map.Entry<EntityMatchType, List<MatchKeyTuple>> typeEntry : entityEntry.getValue().entrySet()) {
                log.debug("    " + typeEntry.getKey() + ":");
                if (CollectionUtils.isEmpty(typeEntry.getValue())) {
                    log.debug("      <empty> (for " + typeEntry.getKey() + ")");
                    continue;
                }
                for (MatchKeyTuple tuple : typeEntry.getValue()) {
                    log.debug("      " + tuple);
                }
            }
        }
    }
}
