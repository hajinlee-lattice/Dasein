package com.latticeengines.matchapi.testframework;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.STAGING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * class for verifying entity match results and current account universe (making
 * sure all requirements/constraints are satisfied)
 */
public class AdvancedAccountMatchTestResultVerifier {
    private static final Logger log = LoggerFactory.getLogger(AdvancedAccountMatchTestResultVerifier.class);

    public static final String COL_TEST_GRP_TYPE = "TestGroupType";
    public static final String COL_TEST_GRP_ID = "TestGroupId";
    public static final String COL_TEST_RECORD_ID = "TestRecordId";
    public static final String COL_EXISTS_IN_UNIVERSE = "ExistsInUniverse";
    public static final String COL_ACCOUNT_ID = InterfaceName.CustomerAccountId.name();
    public static final String COL_SFDC_ID = "SfdcId";
    public static final String COL_MKTO_ID = "MktoId";
    public static final String COL_MATCH_ERRORS = "Entity_Match_Error";

    private static final int BATCH_SIZE = 20;
    // use to exclude public domain error message, just in case
    private static final String PUBLIC_DOMAIN_ERROR_MSG = "Parsed to a public domain";
    private static final String CTRL_GRP_NO_CONFLICT = "controlled_no_conflict";
    private static final String CTRL_GRP_CONFLICT = "controlled_conflict";


    // services & fixed values
    private final Tenant tenant;
    private final String entity;
    private final EntityRawSeedService entityRawSeedService;
    private final EntityLookupEntryService entityLookupEntryService;
    private final EntityMatchVersionService entityMatchVersionService;

    // temp states
    private Map<Pair<String, String>, String> entityIdMap = new HashMap<>(); // [ grpId, recordId ] => entityId
    private Map<String, EntityRawSeed> seeds = new HashMap<>();
    private Map<EntityLookupEntry, String> lookupEntries = new HashMap<>();
    private Map<String, Map<String, Record>> testRecords = new HashMap<>(); // grpId => recordId => record
    private List<String> errors = new ArrayList<>();

    public AdvancedAccountMatchTestResultVerifier(@NotNull Tenant tenant, @NotNull String entity,
            @NotNull EntityRawSeedService entityRawSeedService,
            @NotNull EntityLookupEntryService entityLookupEntryService,
            @NotNull EntityMatchVersionService entityMatchVersionService) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(entity);
        Preconditions.checkNotNull(entityLookupEntryService);
        Preconditions.checkNotNull(entityRawSeedService);
        this.tenant = EntityMatchUtils.newStandardizedTenant(tenant);
        this.entity = entity;
        this.entityRawSeedService = entityRawSeedService;
        this.entityLookupEntryService = entityLookupEntryService;
        this.entityMatchVersionService = entityMatchVersionService;
    }

    /*
     * reset all internal state
     */
    public void reset() {
        entityIdMap.clear();
        seeds.clear();
        lookupEntries.clear();
        testRecords.clear();
        errors.clear();
    }

    /*
     * Add records that are pre-populated into the universe before the test
     */
    public void addExistingRecords(@NotNull Iterator<GenericRecord> records) {
        while (records.hasNext()) {
            Record record = getRecord(records.next());
            if (StringUtils.isNotBlank(record.entityId)) {
                entityIdMap.put(Pair.of(record.grpId, record.recordId), record.entityId);
            } else {
                // all test records should be matched to some entity in allocate ID mode
                errors.add(String.format("Record(grp=%s,id=%s) failed to match to any entity", record.grpId,
                        record.recordId));
            }
        }
    }

    /*
     * add records of test results
     */
    public void addTestResults(@NotNull Iterator<GenericRecord> records) {
        // buffer to store seed/lookup entries to be fetched
        List<String> entityIdBuf = new ArrayList<>();
        List<EntityLookupEntry> lookupEntryBuf = new ArrayList<>();
        while (records.hasNext()) {
            Record record = getRecord(records.next());

            testRecords.putIfAbsent(record.grpId, new HashMap<>());
            testRecords.get(record.grpId).put(record.recordId, record);

            String entityId = record.entityId;
            if (DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(entityId)) {
                // skip anonymous account
                continue;
            }

            if (StringUtils.isBlank(entityId)) {
                // all test records should be matched to some entity in allocate ID mode
                errors.add(String.format("Record(grp=%s,id=%s) failed to match to any entity", record.grpId,
                        record.recordId));
            } else if (!seeds.containsKey(entityId)) {
                // not already fetched
                entityIdBuf.add(entityId);
            }

            if (!records.hasNext() || entityIdBuf.size() >= BATCH_SIZE) {
                List<EntityRawSeed> result = entityRawSeedService.get(STAGING, tenant, entity, entityIdBuf,
                        entityMatchVersionService.getCurrentVersion(STAGING, tenant));
                for (int i = 0; i < entityIdBuf.size(); i++) {
                    EntityRawSeed seed = result.get(i);
                    if (seed == null) {
                        errors.add(
                                String.format("Seed (ID=%s) does not exist in current universe", entityIdBuf.get(i)));
                        continue;
                    }
                    seeds.put(seed.getId(), seed);
                    // only put into buffer if the lookup entry is not already fetched
                    lookupEntryBuf.addAll(seed.getLookupEntries().stream()
                            .filter(entry -> !lookupEntries.containsKey(entry)).collect(Collectors.toList()));
                }
                entityIdBuf.clear();
            }

            if (!records.hasNext() || lookupEntryBuf.size() >= BATCH_SIZE) {
                List<String> result = entityLookupEntryService.get(STAGING, tenant, lookupEntryBuf,
                        entityMatchVersionService.getCurrentVersion(STAGING, tenant));
                for (int i = 0; i < lookupEntryBuf.size(); i++) {
                    lookupEntries.put(lookupEntryBuf.get(i), result.get(i));
                    if (StringUtils.isBlank(result.get(i))) {
                        // every lookup entry in seed should be mapped to some entity
                        errors.add(
                                String.format("Lookup entry [%s] does not map to any entity", lookupEntryBuf.get(i)));
                    }
                }
                lookupEntryBuf.clear();
            }
        }
    }

    /*
     * add records of match errors, should be called after all test results are
     * added
     */
    public void addMatchErrorResults(@NotNull Iterator<GenericRecord> matchErrorRecords) {
        while (matchErrorRecords.hasNext()) {
            GenericRecord genericRecord = matchErrorRecords.next();
            Record record = getRecord(genericRecord);
            if (!testRecords.containsKey(record.grpId) || !testRecords.get(record.grpId).containsKey(record.recordId)) {
                errors.add(String.format("Test result for match error record(grp=%s,id=%s) is not added", record.grpId,
                        record.recordId));
                continue;
            }

            testRecords.get(record.grpId).get(record.recordId).hasConflictError = hasConflictError(genericRecord);
        }
    }

    /*
     * [ grpId, recordId ] => entityId (given test records have to be matched to the
     * given entity ID)
     */
    public void addEntityIdMapping(@NotNull Map<Pair<String, String>, String> map) {
        entityIdMap.putAll(map);
    }

    /*
     * verify all added test results & match errors, also print all errors for
     * debugging
     */
    public void verify() {
        verifyUniverse();
        verifyHighestPriorityKey();
        verifyKnownEntityIds();
        verifyControlGroups();
        logError();
        Assert.assertTrue(errors.isEmpty());
    }

    /*
     * get the error logs
     */
    public List<String> getErrors() {
        return errors;
    }

    /*
     * verify constraints on seed/lookup
     */
    private void verifyUniverse() {
        // [ systemName, systemId ] => list(entityId)
        Map<Pair<String, String>, Set<String>> systemIdMap = new HashMap<>();
        for (EntityRawSeed seed : seeds.values()) {
            if (DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(seed.getId())) {
                // skip anonymous account
                continue;
            }
            Map<String, Integer> systemCnts = new HashMap<>(); // systemName => cnt in this seed (should be <= 1)
            int dunsCnt = 0; // number of duns in this seed (should be <= 1)

            // make sure at least one of the lookup entry mapped back to the seed
            Optional<EntityLookupEntry> entryMappedToCurrentSeed = seed.getLookupEntries().stream()
                    .filter(entry -> lookupEntries.containsKey(entry) && lookupEntries.get(entry).equals(seed.getId()))
                    .findAny();
            if (!entryMappedToCurrentSeed.isPresent()) {
                errors.add(
                        String.format("Seed(ID=%s) does not contain any match key that maps to itself", seed.getId()));
            }

            for (EntityLookupEntry entry : seed.getLookupEntries()) {
                if (entry.getType() == EntityLookupEntry.Type.EXTERNAL_SYSTEM) {
                    Pair<String, String> sys = EntityLookupEntryConverter.toExternalSystem(entry);
                    systemCnts.put(sys.getKey(), systemCnts.getOrDefault(sys.getKey(), 0));

                    systemIdMap.putIfAbsent(sys, new HashSet<>());
                    // [ systemName, systemID ] occurs in this seed
                    systemIdMap.get(sys).add(seed.getId());
                } else if (entry.getType() == EntityLookupEntry.Type.DUNS) {
                    dunsCnt++;
                }
            }

            // check that one seed can only have one ID per system
            systemCnts.entrySet().stream().filter(entry -> entry.getValue() > 1).forEach(entry -> errors
                    .add(String.format("System %s has more than one ID in seed(ID=%s)", entry.getKey(), seed.getId())));
            // check that one seed can only have one DUNS
            if (dunsCnt > 1) {
                errors.add(String.format("Seed(ID=%s) has more than one DUNS", seed.getId()));
            }
        }

        // make sure that the same [ systemName, systemId ] pair can only be in one seed
        systemIdMap.entrySet().stream().filter(entry -> entry.getValue().size() > 1)
                .forEach(entry -> errors.add(String.format("System=%s,ID=%s occurs in multiple seeds, seed IDs = %s",
                        entry.getKey().getLeft(), entry.getKey().getRight(), entry.getValue())));

        List<Map.Entry<EntityLookupEntry, String>> orphanEntries = lookupEntries.entrySet().stream()
                .filter(entry -> !seeds.containsKey(entry.getValue())).collect(Collectors.toList());
        if (!orphanEntries.isEmpty()) {
            errors.add(String.format(
                    "Entities mapped by the following match keys does not exist in current universe, lookup entries = %s",
                    orphanEntries));
        }
    }

    /*
     * make sure for every test record, the highest priority match key mapped to the
     * same entity as the match result
     *
     * NOTE only verify system ID for now because other fields need to be
     * standardized before lookup
     */
    private void verifyHighestPriorityKey() {
        // iterate through every test record
        testRecords.values().stream().flatMap(groupRecords -> groupRecords.values().stream()).forEach(record -> {
            if (DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(record.entityId)) {
                // skip anonymous account
                return;
            }
            // only check system ID for now
            Pair<String, String> sys = null;
            if (StringUtils.isNotBlank(record.accountId)) {
                sys = Pair.of(COL_ACCOUNT_ID, record.accountId);
            } else if (StringUtils.isNotBlank(record.sfdcId)) {
                sys = Pair.of(COL_SFDC_ID, record.sfdcId);
            } else if (StringUtils.isNotBlank(record.mktoId)) {
                sys = Pair.of(COL_MKTO_ID, record.mktoId);
            }

            if (sys != null) {
                EntityLookupEntry entry = EntityLookupEntryConverter.fromSystemId(entity, sys.getKey(), sys.getValue());
                if (!record.entityId.equals(lookupEntries.get(entry))) {
                    errors.add(String.format(
                            "Highest priority key %s for record(grp=%s,id=%s) does not map to the same entity(ID=%s) as the match result",
                            entry, record.grpId, record.recordId, record.entityId));
                }
            }
        });
    }

    /*
     * make sure all specified records match to the correct entity ID
     */
    private void verifyKnownEntityIds() {
        if (entityIdMap.isEmpty()) {
            return;
        }
        testRecords.values().stream().flatMap(groupRecords -> groupRecords.values().stream()).forEach(record -> {
            Pair<String, String> pair = Pair.of(record.grpId, record.recordId);
            if (entityIdMap.containsKey(pair) && !record.entityId.equals(entityIdMap.get(pair))) {
                errors.add(String.format(
                        "Test record(grp=%s,id=%s) is supposed to map to entity(ID=%s), but get entity(ID=%s) in match result",
                        record.grpId, record.recordId, entityIdMap.get(pair), record.entityId));
            }
        });
    }

    /*
     * verify groups that has more predictable match results
     */
    private void verifyControlGroups() {
        testRecords.forEach((grpId, records) -> {
            if (records.isEmpty()) {
                return;
            }

            String grpType = records.values().iterator().next().grpType;
            if (CTRL_GRP_NO_CONFLICT.equals(grpType)) {
                Set<String> entityIds = getEntityIds(records.values());
                if (entityIds.size() > 1) {
                    errors.add(String.format("Records in test group(ID=%s) should all be matched to the same entity. "
                            + "However, multiple entity IDs are returned: %s", grpId, entityIds));
                }
                Set<String> recordIdsWithConflict = records.values().stream() //
                        .filter(record -> record.hasConflictError) //
                        .map(record -> record.recordId) //
                        .collect(Collectors.toSet());
                if (!recordIdsWithConflict.isEmpty()) {
                    errors.add(String.format(
                            "Records in test group(ID=%s) should not have conflict. Conflict record IDs = %s", grpId,
                            recordIdsWithConflict));
                }
            } else if (CTRL_GRP_CONFLICT.equals(grpType)) {
                // TODO check control group 2 & 3 when data generation script is enhanced to give more information
            }
        });
    }

    private Set<String> getEntityIds(Collection<Record> records) {
        return records.stream().map(record -> record.entityId).collect(Collectors.toSet());
    }

    /*
     * log all verification error, has to be executed after verify() is invoked
     */
    private void logError() {
        if (errors.isEmpty()) {
            return;
        }

        log.warn("====================================");
        log.warn("Verification errors for entity={}, tenant(ID={}):", entity, tenant.getId());
        errors.forEach(log::warn);
        log.warn("====================================");
    }

    private Record getRecord(@NotNull GenericRecord genericRecord) {
        Preconditions.checkNotNull(genericRecord);
        Record record = new Record();
        record.grpType = get(genericRecord, COL_TEST_GRP_TYPE);
        record.grpId = get(genericRecord, COL_TEST_GRP_ID);
        record.recordId = get(genericRecord, COL_TEST_RECORD_ID);
        // has to have these fields, otherwise test is not setup properly
        Preconditions.checkNotNull(record.grpType);
        Preconditions.checkNotNull(record.grpId);
        Preconditions.checkNotNull(record.recordId);

        record.existsInUniverse = getBool(genericRecord, COL_EXISTS_IN_UNIVERSE);
        record.accountId = get(genericRecord, COL_ACCOUNT_ID);
        record.sfdcId = get(genericRecord, COL_SFDC_ID);
        record.mktoId = get(genericRecord, COL_MKTO_ID);
        record.name = get(genericRecord, MatchKey.Name.name());
        record.domain = get(genericRecord, MatchKey.Domain.name());
        record.duns = get(genericRecord, MatchKey.DUNS.name());
        record.country = get(genericRecord, MatchKey.Country.name());
        record.state = get(genericRecord, MatchKey.State.name());
        record.city = get(genericRecord, MatchKey.City.name());
        record.entityId = get(genericRecord, InterfaceName.EntityId.name());
        return record;
    }

    private boolean hasConflictError(@NotNull GenericRecord record) {
        String errStr = get(record, COL_MATCH_ERRORS);
        if (StringUtils.isEmpty(errStr)) {
            return false;
        }

        // exclude the case where error message contain only one public domain error (no
        // delimiter | and start with public domain error message prefix)
        return !errStr.startsWith(PUBLIC_DOMAIN_ERROR_MSG) || errStr.indexOf('|') != -1;
    }

    private String get(@NotNull GenericRecord record, @NotNull String col) {
        // transform from Utf8 to string if needed
        return record.get(col) == null ? null : record.get(col).toString();
    }

    private boolean getBool(@NotNull GenericRecord record, @NotNull String col) {
        Object obj = record.get(col);
        if (obj == null) {
            return false;
        }
        String str = obj.toString();
        return Boolean.TRUE.toString().equalsIgnoreCase(str);
    }

    /*
     * class to hold test record & match result
     */
    private class Record {
        // test metadata fields
        String grpType;
        String grpId;
        String recordId;
        boolean existsInUniverse;
        boolean hasConflictError;

        // system ID fields
        String accountId;
        String sfdcId;
        String mktoId;

        // other match key fields
        String name;
        String domain;
        String duns;
        String country;
        String state;
        String city;

        // matched entity ID
        String entityId;
    }
}
