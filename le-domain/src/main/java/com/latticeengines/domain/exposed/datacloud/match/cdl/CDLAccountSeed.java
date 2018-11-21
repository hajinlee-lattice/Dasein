package com.latticeengines.domain.exposed.datacloud.match.cdl;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class to represent seed for CDL account.
 */
public class CDLAccountSeed {
    private static final CDLMatchEntity ENTITY = CDLMatchEntity.ACCOUNT;
    private static final String KEY_LATTICE_ACCOUNT_ID = "latticeAccountId";

    private final String id;
    private final String latticeAccountId;
    private final Map<String, String> externalSystemIdMap; // System name => System ID
    private final String duns;
    private final Set<Pair<String, String>> domainCountries; // Pair<Domain, Country>
    private final Set<Pair<String, String>> nameCountries; // Pair<Name, Country>

    public CDLAccountSeed(
            @NotNull String id, String latticeAccountId, Map<String, String> externalSystemIdMap, String duns,
            Set<Pair<String, String>> domainCountries, Set<Pair<String, String>> nameCountries) {
        Preconditions.checkNotNull(id);
        this.id = id;
        this.latticeAccountId = latticeAccountId;
        this.externalSystemIdMap = externalSystemIdMap == null ? Collections.emptyMap() : externalSystemIdMap;
        this.duns = duns;
        this.domainCountries = domainCountries == null ? Collections.emptySet() : domainCountries;
        this.nameCountries = nameCountries == null ? Collections.emptySet() : nameCountries;
    }

    public String getSystemId(@NotNull String systemName) {
        Preconditions.checkNotNull(systemName);
        return externalSystemIdMap.get(systemName);
    }

    public Map<String, String> getExternalSystemIdMap() {
        return externalSystemIdMap;
    }

    public String getDuns() {
        return duns;
    }

    public Set<Pair<String, String>> getDomainCountries() {
        return domainCountries;
    }

    public Set<Pair<String, String>> getNameCountries() {
        return nameCountries;
    }

    /**
     * Convert this CDL Account seed to raw seed for internal operations.
     * TODO find out if we need this method or not (probably not)
     *
     * @param comparator sort lookup entries by their priority, in DESC order (from high to low)
     * @return generated {@link CDLRawSeed} will not be {@literal null}
     */
    public CDLRawSeed toRawSeed(@NotNull Comparator<CDLLookupEntry> comparator) {
        Preconditions.checkNotNull(comparator);
        List<CDLLookupEntry> lookupEntries = new ArrayList<>();
        if (!externalSystemIdMap.isEmpty()) {
            lookupEntries.addAll(externalSystemIdMap
                    .entrySet()
                    .stream()
                    .map(entry -> CDLLookupEntryConverter.fromExternalSystem(ENTITY, entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList()));
        }
        if (StringUtils.isNotBlank(duns)) {
            lookupEntries.add(CDLLookupEntryConverter.fromDuns(ENTITY, duns));
        }
        if (!domainCountries.isEmpty()) {
            lookupEntries.addAll(domainCountries
                    .stream()
                    .map(entry -> CDLLookupEntryConverter.fromDomainCountry(ENTITY, entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList()));
        }
        if (!nameCountries.isEmpty()) {
            lookupEntries.addAll(nameCountries
                    .stream()
                    .map(entry -> CDLLookupEntryConverter.fromNameCountry(ENTITY, entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList()));
        }
        // sort base on order
        lookupEntries.sort(comparator);
        return new CDLRawSeed(id, ENTITY, lookupEntries,
                StringUtils.isBlank(latticeAccountId)
                ? null
                : Collections.singletonMap(KEY_LATTICE_ACCOUNT_ID, latticeAccountId));
    }

    /**
     * Create a new instance of {@link CDLAccountSeed} from given {@link CDLRawSeed}
     *
     * @param rawSeed raw seed to transform from, should not be {@literal null} and
     *                should have the correct {@link CDLMatchEntity}
     * @return generated {@link CDLAccountSeed}
     */
    public static CDLAccountSeed fromRawSeed(@NotNull CDLRawSeed rawSeed) {
        Preconditions.checkNotNull(rawSeed);
        Preconditions.checkArgument(rawSeed.getEntity() == ENTITY);
        CDLAccountSeedBuilder builder = new CDLAccountSeedBuilder();
        // set CDL account ID and lattice account ID
        builder.withId(rawSeed.getId()).withLatticeAccountId(rawSeed.getAttributes().get(KEY_LATTICE_ACCOUNT_ID));
        rawSeed.getLookupEntries().forEach(entry -> {
            // TODO check for data integrity
            switch (entry.getType()) {
                case DUNS:
                    builder.withDuns(CDLLookupEntryConverter.toDuns(entry));
                    break;
                case NAME_COUNTRY:
                    builder.addNameCountryPair(CDLLookupEntryConverter.toNameCountry(entry));
                    break;
                case DOMAIN_COUNTRY:
                    builder.addDomainCountryPair(CDLLookupEntryConverter.toDomainCountry(entry));
                    break;
                case EXTERNAL_SYSTEM:
                    builder.addExternalSystemIdPair(CDLLookupEntryConverter.toExternalSystem(entry));
                    break;
                default:
                    throw new UnsupportedOperationException("Lookup entry type is not supported: " + entry.getType());
            }
        });
        return builder.build();
    }
}
