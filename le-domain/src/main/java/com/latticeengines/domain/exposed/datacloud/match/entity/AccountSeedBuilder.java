package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Builder for {@link AccountSeed}
 */
public final class AccountSeedBuilder {
    private String id;
    private String latticeAccountId;
    private Map<String, String> externalSystemIdMap;
    private String duns;
    private Set<Pair<String, String>> domainCountries;
    private Set<Pair<String, String>> nameCountries;

    public AccountSeedBuilder() {
    }

    public AccountSeedBuilder withId(String id) {
        this.id = id;
        return this;
    }

    public AccountSeedBuilder withLatticeAccountId(String latticeAccountId) {
        this.latticeAccountId = latticeAccountId;
        return this;
    }

    public AccountSeedBuilder withExternalSystemIdMap(Map<String, String> externalSystemIdMap) {
        this.externalSystemIdMap = externalSystemIdMap;
        return this;
    }

    public AccountSeedBuilder addExternalSystemIdPair(Pair<String, String> pair) {
        return addExternalSystemIdPair(pair.getLeft(), pair.getRight());
    }

    public AccountSeedBuilder addExternalSystemIdPair(String systemName, String systemId) {
        if (externalSystemIdMap == null) {
            externalSystemIdMap = new HashMap<>();
        }
        externalSystemIdMap.put(systemName, systemId);
        return this;
    }

    public AccountSeedBuilder withDuns(String duns) {
        this.duns = duns;
        return this;
    }

    public AccountSeedBuilder withDomainCountries(Set<Pair<String, String>> domainCountries) {
        this.domainCountries = domainCountries;
        return this;
    }

    public AccountSeedBuilder addDomainCountryPair(Pair<String, String> pair) {
        return addDomainCountryPair(pair.getLeft(), pair.getRight());
    }

    public AccountSeedBuilder addDomainCountryPair(String domain, String country) {
        Preconditions.checkArgument(domain != null || country != null);
        if (domainCountries == null) {
            domainCountries = new HashSet<>();
        }
        domainCountries.add(Pair.of(domain, country));
        return this;
    }

    public AccountSeedBuilder withNameCountries(Set<Pair<String, String>> nameCountries) {
        this.nameCountries = nameCountries;
        return this;
    }

    public AccountSeedBuilder addNameCountryPair(Pair<String, String> pair) {
        return addNameCountryPair(pair.getLeft(), pair.getRight());
    }

    public AccountSeedBuilder addNameCountryPair(String name, String country) {
        Preconditions.checkArgument(name != null || country != null);
        if (nameCountries == null) {
            nameCountries = new HashSet<>();
        }
        nameCountries.add(Pair.of(name, country));
        return this;
    }

    public AccountSeed build() {
        return new AccountSeed(id, latticeAccountId, externalSystemIdMap, duns, domainCountries, nameCountries);
    }
}
