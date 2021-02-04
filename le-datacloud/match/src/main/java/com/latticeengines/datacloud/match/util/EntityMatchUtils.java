package com.latticeengines.datacloud.match.util;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ENTITY_PREFIX_SEED_ATTRIBUTES;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.MATCH_FIELD_LENGTH_LIMIT;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.MANY_TO_MANY;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.ONE_TO_ONE;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.STAGING;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Utility functions for entity match
 */
public final class EntityMatchUtils {

    private static final Logger log = LoggerFactory.getLogger(EntityMatchUtils.class);

    private static final BiMap<String, String> INVALID_CHARS_TOKENS = HashBiMap.create();
    // # and : is recommended not to use by dynamo, || is our internal delimiter
    private static final Pattern INVALID_MATCH_FILED_CHAR_PTN = Pattern.compile("(#|:|\\|\\|)");
    private static final Pattern INVALID_CHAR_TOKEN_PTN = Pattern.compile("\\$>(PND|COLON|DPIPE)<");
    private static final String COMMIT_ENTITY_MATCH_STAGING_LOCK = "COMMIT_EM_STAGING_LOCK";
    private static final long DEFAULT_LOCK_WAIT_TIME_IN_HOURS = 2L;

    protected EntityMatchUtils() {
        throw new UnsupportedOperationException();
    }

    // Match Target Entity -> Set(Entity that we will output a list of newly
    // allocated IDs)
    private static final Map<String, Set<String>> OUTPUT_NEW_ENTITY_MAP = new HashMap<>();
    // Match Target Entity -> Set(Attribute names that will only be set when this
    // attribute does not exist in seed)
    private static final Map<String, Set<String>> FIRST_WIN_ATTRIBUTES = new HashMap<>();

    static {
        OUTPUT_NEW_ENTITY_MAP.put(Account.name(), Sets.newHashSet(Account.name()));
        OUTPUT_NEW_ENTITY_MAP.put(Contact.name(), Sets.newHashSet(Account.name(), Contact.name()));
        // currently only LDC ID in account match is first win, others are last win
        FIRST_WIN_ATTRIBUTES.put(Account.name(), Collections.singleton(InterfaceName.LatticeAccountId.name()));

        // invalid char pattern -> placeholder token
        INVALID_CHARS_TOKENS.put("#", "\\$>PND<");
        INVALID_CHARS_TOKENS.put(":", "\\$>COLON<");
        INVALID_CHARS_TOKENS.put("\\|\\|", "\\$>DPIPE<");
    }

    public static void overwriteWithConfiguration(@NotNull EntityMatchConfigurationService service,
            EntityMatchConfiguration config) {
        if (config == null) {
            return;
        }

        // overwrite configuration
        if (config.getNumStagingShards() != null) {
            service.setNumShards(EntityMatchEnvironment.STAGING, config.getNumStagingShards());
        }
        if (StringUtils.isNotBlank(config.getStagingTableName())) {
            service.setStagingTableName(config.getStagingTableName());
        }
        if (config.isLazyCopyToStaging() != null) {
            service.setShouldCopyToStagingLazily(config.isLazyCopyToStaging());
        }
    }

    public static void setPerEntityAllocationModes(@NotNull EntityMatchConfigurationService service,
            @NotNull EntityMatchConfiguration config, String srcEntity, boolean logResult) {
        if (MapUtils.isEmpty(config.getAllocationModes()) || StringUtils.isBlank(srcEntity)) {
            return;
        }
        config.getAllocationModes().entrySet().stream() //
                .filter(e -> srcEntity.equals(e.getKey())) //
                .findAny() //
                .ifPresent(e -> {
                    Map<String, Boolean> allocationModes = e.getValue();
                    if (logResult) {
                        log.info("Setting per entity allocation modes = {} for Entity {}", allocationModes, srcEntity);
                    }
                    service.setPerEntityAllocationModes(allocationModes);
                });
    }

    /**
     * Acquire lock to perform the operation to publishing seed/lookup entries from
     * staging env to serving env
     *
     * @return true if lock is acquired
     */
    public static boolean lockCommitStep() {
        return lockCommitStep(DEFAULT_LOCK_WAIT_TIME_IN_HOURS, TimeUnit.HOURS);
    }

    /**
     * Acquire lock to perform the operation of publishing seed/lookup entries from
     * staging env to serving env
     *
     * @param duration
     *            duration to wait when lock cannot be acquired
     * @param unit
     *            time unit of duration
     * @return true if lock is acquired
     */
    public static boolean lockCommitStep(long duration, TimeUnit unit) {
        String lockName = COMMIT_ENTITY_MATCH_STAGING_LOCK;
        LockManager.registerCrossDivisionLock(lockName);
        return LockManager.acquireWriteLock(lockName, duration, unit);
    }

    /**
     * Release lock for operation to publishing seed/lookup entries from staging env
     * to serving env. Noop if lock is not acquired
     */
    public static void unlockCommitStep() {
        LockManager.releaseWriteLock(COMMIT_ENTITY_MATCH_STAGING_LOCK);
    }

    /**
     * Generate a (somewhat) user friendly string for the input
     * {@link EntityLookupEntry}
     *
     * @param entry
     *            target entry
     * @return string representation of the entry
     */
    public static String prettyToString(EntityLookupEntry entry) {
        if (entry == null) {
            return StringUtils.EMPTY;
        }
        String[] values;
        switch (entry.getType()) {
        case DUNS:
            return String.format("{DUNS=%s}", entry.getSerializedValues());
        case NAME_COUNTRY:
            values = entry.getValues();
            return String.format("{Name=%s,Country=%s}", values[0], values[1]);
        case DOMAIN_COUNTRY:
            values = entry.getValues();
            return String.format("{Domain=%s,Country=%s}", values[0], values[1]);
        case EXTERNAL_SYSTEM:
            return String.format("{System=%s,ID=%s}", entry.getSerializedKeys(), entry.getSerializedValues());
        default:
            return StringUtils.EMPTY;
        }
    }

    /**
     * Serialize version map, only consider supported environments
     *
     * @param versionMap
     *            target version map, nullable
     * @return serialized representation of given map
     */
    public static String serialize(Map<EntityMatchEnvironment, Integer> versionMap) {
        return String.format("%d_%d", MapUtils.getObject(versionMap, STAGING), MapUtils.getObject(versionMap, SERVING));
    }

    /**
     * Merge a change seed into a base seed, excluding a set of known lookup entries
     * that will cause conflict.
     *
     * @param base
     *            target seed that we want to update
     * @param change
     *            seed that contains all match keys used to update base seed
     * @param conflictEntries
     *            set of entries that will NOT be merged into base seed
     * @return merged seed, will not be {@literal null}
     */
    public static EntityRawSeed mergeSeed(@NotNull EntityRawSeed base, EntityRawSeed change,
            Set<EntityLookupEntry> conflictEntries) {
        Preconditions.checkNotNull(base, "Base seed to be updated should not be null");
        if (change == null) {
            return base;
        }

        Map<String, String> attrs = new HashMap<>(base.getAttributes());
        Set<EntityLookupEntry> entries = new HashSet<>(base.getLookupEntries());
        // NOTE: assumption here is that the number of match keys will not be large, so
        // using iteration to check conflict is better than having separate map/set
        change.getLookupEntries().forEach(entry -> {
            if (isNotEmpty(conflictEntries) && conflictEntries.contains(entry)
                    && entry.getType().mapping == ONE_TO_ONE) {
                // conflict known prior to update, no need to merge
                return;
            }
            // if many to many, just update as equals() in lookup entry will handle the
            // dedup. otherwise, only update if there is no conflict.
            if (entry.getType().mapping == MANY_TO_MANY || !hasConflictInSeed(base, entry)) {
                entries.add(entry);
            }
        });
        change.getAttributes().forEach((attrName, attrVal) -> {
            if (shouldOverrideAttribute(base.getEntity(), attrName)) {
                attrs.put(attrName, attrVal);
            } else {
                attrs.putIfAbsent(attrName, attrVal);
            }
        });
        return new EntityRawSeed(base.getId(), base.getEntity(), base.isNewlyAllocated(), base.getVersion(),
                new ArrayList<>(entries), attrs);
    }

    /**
     * Check if input entry has conflict with target seed (same serialized key,
     * different value). Only evaluate entries with X to one mapping.
     *
     * @param seed
     *            target seed
     * @param entry
     *            entry to be evaluated
     * @return true if the input entry has conflict with target seed
     */
    public static boolean hasConflictInSeed(@NotNull EntityRawSeed seed, @NotNull EntityLookupEntry entry) {
        Preconditions.checkNotNull(seed);
        Preconditions.checkNotNull(entry);
        if (entry.getType().mapping == MANY_TO_MANY) {
            // only consider x to one
            return false;
        }

        Optional<EntityLookupEntry> conflictEntry = seed.getLookupEntries().stream()
                .filter(entryInSeed -> entryInSeed.getType() == entry.getType())
                .filter(entryInSeed -> Objects.equals(entryInSeed.getSerializedKeys(), entry.getSerializedKeys()))
                .filter(entryInSeed -> !Objects.equals(entryInSeed.getSerializedValues(), entry.getSerializedValues()))
                .findAny();
        return conflictEntry.isPresent();
    }

    /**
     * Create a new {@link Tenant} with ID fields standardized
     *
     * @param tenant
     *            input tenant, must not be {@literal null} and contains non-null ID
     * @return non null tenant with standardized ID field
     */
    public static Tenant newStandardizedTenant(@NotNull Tenant tenant) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(tenant.getId());
        return new Tenant(CustomerSpace.parse(tenant.getId()).getTenantId());
    }

    /**
     * Based on the input environment, determine whether we should set TTL for
     * seed/lookup entries or not.
     *
     * @param env
     *            input environment
     * @return true if we should set TTL
     */
    public static boolean shouldSetTTL(EntityMatchEnvironment env) {
        // at the moment, only set TTL for staging environment
        return env == STAGING;
    }

    /**
     * Determine whether it needs to output a list of newly allocated entities.
     *
     * @param input
     *            target match input
     * @return true if needed to output, false otherwise
     */
    public static boolean shouldOutputNewEntities(MatchInput input) {
        return isAllocateIdModeEntityMatch(input) && input.isOutputNewEntities()
                && OUTPUT_NEW_ENTITY_MAP.containsKey(input.getTargetEntity());
    }

    /**
     * Determine whether we should output current entity given the original target
     * entity in {@link MatchInput}
     *
     * @param input
     *            original match input instance
     * @param currentEntity
     *            entity for current decision graph
     * @return true if we want to output newly allocated current entity, false
     *         otherwise.
     */
    public static boolean shouldOutputNewEntity(MatchInput input, String currentEntity) {
        return shouldOutputNewEntities(input) && OUTPUT_NEW_ENTITY_MAP.containsKey(input.getTargetEntity())
                && OUTPUT_NEW_ENTITY_MAP.get(input.getTargetEntity()).contains(currentEntity);
    }

    /**
     * Helper to determine from current {@link MatchInput} whether it is entity
     * match and is in allocateId mode.
     *
     * @param input
     *            target match input object
     * @return true if it is entity match and is in allocate mode
     */
    public static boolean isAllocateIdModeEntityMatch(MatchInput input) {
        if (input == null) {
            return false;
        }

        // use the value in config first, fallback to value in input if not provided
        Boolean allocateIdInConfig = input.getEntityMatchConfiguration() != null
                ? input.getEntityMatchConfiguration().getAllocateId()
                : null;
        return OperationalMode.isEntityMatch(input.getOperationalMode())
                && ObjectUtils.defaultIfNull(allocateIdInConfig, input.isAllocateId());
    }

    /**
     * Determine whether the target tuple has only System IDs match key combination.
     * I.e., only {@link EntityLookupEntry.Type#EXTERNAL_SYSTEM} will be able to
     * transform the tuple to {@link EntityLookupEntry}.
     *
     * @param tuple
     *            target tuple
     * @return true if only systemIds
     */
    public static boolean hasSystemIdsOnly(MatchKeyTuple tuple) {
        if (tuple == null) {
            return false;
        }

        return isNotEmpty(tuple.getSystemIds()) && isBlank(tuple.getEmail()) && isBlank(tuple.getName())
                && isBlank(tuple.getPhoneNumber());
    }

    /**
     * Determine whether email field is mapped in current match and is the only
     * field used as domain in account match.
     *
     * @param traveler
     *            current traveler object
     * @return true if no other account info than Email
     */
    public static boolean hasEmailAccountInfoOnly(@NotNull MatchTraveler traveler) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        MatchKeyTuple accountTuple = traveler.getEntityMatchKeyTuple(Account.name());
        if (tuple == null) {
            // be definsive for now, these two should not be null, maybe fail later
            return false;
        }

        // Assumption is: If Contact has Email, it must be mapped in
        // Account Domain match key because in Account match, there is
        // no concept of "Email", thus we can only detect how many
        // domain fields are mapped
        return tuple.getEmail() != null
                && (getValidEntityId(Account.name(), traveler) == null || (accountTuple != null
                        && accountTuple.hasDomainOnly() && !accountTuple.isDomainFromMultiCandidates()));
    }

    /**
     * Determine whether we should override target attribute value or only set if
     * the attribute does not exist
     *
     * @param entity
     *            target entity in current match
     * @param attrName
     *            attribute name
     * @return true if we should override, false if we should set only when not
     *         exist
     */
    public static boolean shouldOverrideAttribute(@NotNull String entity, @NotNull String attrName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entity),
                String.format("Target entity=%s should not be blank", entity));
        Preconditions.checkArgument(StringUtils.isNotBlank(attrName),
                String.format("Attribute name [%s] should not be blank", attrName));
        if (attrName.startsWith(ENTITY_PREFIX_SEED_ATTRIBUTES)) {
            attrName = attrName.substring(ENTITY_PREFIX_SEED_ATTRIBUTES.length());
        }

        // attrs not in firstWinMap are considered last win (override with new value)
        return !FIRST_WIN_ATTRIBUTES.getOrDefault(entity, Collections.emptySet()).contains(attrName);
    }

    /**
     * Return valid matched entity ID (ID of a non-anonymous entity) of specified
     * entity
     *
     * @param entity
     *            target entity
     * @param traveler
     *            traveler instance
     * @return valid entity ID, {@literal null} if no valid ID found
     */
    public static String getValidEntityId(@NotNull String entity, MatchTraveler traveler) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entity),
                String.format("Target entity=%s should not be blank", entity));
        if (traveler == null || MapUtils.isEmpty(traveler.getEntityIds())) {
            return null;
        }

        String entityId = traveler.getEntityIds().get(entity);
        if (isBlank(entityId) || DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(entityId)) {
            return null;
        }
        return entityId;
    }

    /**
     * Get match key -> column name map for target entity in the given input.
     *
     * @param input
     *            match input object
     * @param entity
     *            target entity
     * @return map for target entity, return an empty map if no such map exists
     */
    public static Map<MatchKey, List<String>> getKeyMapForEntity(MatchInput input, @NotNull String entity) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entity),
                String.format("Target entity=%s should not be blank", entity));
        if (input == null || MapUtils.isEmpty(input.getEntityKeyMaps())) {
            return Collections.emptyMap();
        }
        if (input.getEntityKeyMaps().get(entity) == null) {
            return Collections.emptyMap();
        }

        Map<MatchKey, List<String>> keyMap = input.getEntityKeyMaps().get(entity).getKeyMap();
        return keyMap == null ? new HashMap<>() : keyMap;
    }

    /**
     * Replace all invalid characters in each match field with a placeholder token
     *
     * @param tuple
     *            target match keys
     * @return match key tuple with invalid characters replaced
     */
    public static MatchKeyTuple replaceInvalidMatchFieldCharacters(MatchKeyTuple tuple) {
        if (tuple == null) {
            return null;
        }

        processMatchKeyTuple(tuple, EntityMatchUtils::replaceInvalidMatchFieldCharacters);
        return tuple;
    }

    /**
     * Whether input value is valid to be used as id of some {@link BusinessEntity}
     *
     * @param entityId
     *            input value
     * @return whether it is valid
     */
    public static boolean isValidEntityId(@NotNull String entityId) {
        if (StringUtils.isBlank(entityId)) {
            return false;
        }

        // within limit and no invalid characters
        return !INVALID_MATCH_FILED_CHAR_PTN.matcher(entityId).find() && entityId.length() <= MATCH_FIELD_LENGTH_LIMIT;
    }

    /**
     * Hash long match key tuple to a shorter value
     *
     * @param tuple
     *            target match keys
     * @return processed tuple with all long field hashed
     */
    public static MatchKeyTuple hashLongMatchFields(MatchKeyTuple tuple) {
        if (tuple == null) {
            return null;
        }

        processMatchKeyTuple(tuple, EntityMatchUtils::hashIfLong);
        return tuple;
    }

    /**
     * Replace invalid characters in match field value with corresponding
     * placeholder
     *
     * @param value
     *            match field value
     * @return replaced value without any invalid characters
     */
    public static String replaceInvalidMatchFieldCharacters(String value) {
        return replaceAllPatterns(value, INVALID_CHARS_TOKENS, INVALID_MATCH_FILED_CHAR_PTN);
    }

    /**
     * Restore all placeholder token back to original (invalid) character
     *
     * @param replacedValue
     *            replaced match field value
     * @return original match field value
     */
    public static String restoreInvalidMatchFieldCharacters(String replacedValue) {
        return replaceAllPatterns(replacedValue, INVALID_CHARS_TOKENS.inverse(), INVALID_CHAR_TOKEN_PTN);
    }

    /**
     * Restore all values (entity IDs) in map back to its original values
     *
     * @param entityIds
     *            entity -> id map
     * @return map of entity -> original ID value
     */
    public static Map<String, String> restoreInvalidMatchFieldCharacters(Map<String, String> entityIds) {
        if (MapUtils.isEmpty(entityIds)) {
            return entityIds;
        }

        return entityIds.entrySet() //
                .stream() //
                .map(entry -> Pair.of(entry.getKey(), restoreInvalidMatchFieldCharacters(entry.getValue()))) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private static void processMatchKeyTuple(MatchKeyTuple tuple, Function<String, String> fn) {
        if (tuple == null || fn == null) {
            return;
        }

        tuple.setZipcode(fn.apply(tuple.getZipcode()));
        tuple.setCity(fn.apply(tuple.getCity()));
        tuple.setState(fn.apply(tuple.getState()));
        tuple.setCountry(fn.apply(tuple.getCountry()));
        tuple.setName(fn.apply(tuple.getName()));

        tuple.setDomain(fn.apply(tuple.getDomain()));
        tuple.setDuns(fn.apply(tuple.getDuns()));

        tuple.setEmail(fn.apply(tuple.getEmail()));
        tuple.setPhoneNumber(fn.apply(tuple.getPhoneNumber()));
        if (isNotEmpty(tuple.getSystemIds())) {
            tuple.setSystemIds(tuple.getSystemIds() //
                    .stream() //
                    .map(pair -> Pair.of(fn.apply(pair.getKey()), fn.apply(pair.getValue()))) //
                    .collect(Collectors.toList()));
        }
    }

    private static String hashIfLong(String val) {
        if (StringUtils.length(val) <= DataCloudConstants.MATCH_FIELD_LENGTH_LIMIT) {
            return val;
        }

        return HashUtils.getMD5CheckSum(val);
    }

    /*-
     * if str matches matchingPtn,
     * replace all ptn (keys in ptnTokenMap) with placeholder token (values in ptnTokenMap)
     */
    private static String replaceAllPatterns(String str, BiMap<String, String> ptnTokenMap, Pattern matchingPtn) {
        if (isBlank(str)) {
            return str;
        }

        if (matchingPtn.matcher(str).find()) {
            for (String invalidStr : ptnTokenMap.keySet()) {
                str = str.replaceAll(invalidStr, ptnTokenMap.get(invalidStr));
            }
        }
        return str;
    }
}
