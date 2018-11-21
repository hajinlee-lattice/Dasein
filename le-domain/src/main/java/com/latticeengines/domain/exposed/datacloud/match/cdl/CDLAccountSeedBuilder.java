package com.latticeengines.domain.exposed.datacloud.match.cdl;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Builder for {@link CDLAccountSeed}
 */
public final class CDLAccountSeedBuilder {
    private String id;
    private String latticeAccountId;
    private Map<String, String> externalSystemIdMap;
    private String duns;
    private Set<Pair<String, String>> domainCountries;
    private Set<Pair<String, String>> nameCountries;

    public CDLAccountSeedBuilder() {
    }

    public CDLAccountSeedBuilder withId(String id) {
        this.id = id;
        return this;
    }

    public CDLAccountSeedBuilder withLatticeAccountId(String latticeAccountId) {
        this.latticeAccountId = latticeAccountId;
        return this;
    }

    public CDLAccountSeedBuilder withExternalSystemIdMap(Map<String, String> externalSystemIdMap) {
        this.externalSystemIdMap = externalSystemIdMap;
        return this;
    }

    public CDLAccountSeedBuilder addExternalSystemIdPair(Pair<String, String> pair) {
        return addExternalSystemIdPair(pair.getLeft(), pair.getRight());
    }

    public CDLAccountSeedBuilder addExternalSystemIdPair(String systemName, String systemId) {
        if (externalSystemIdMap == null) {
            externalSystemIdMap = new HashMap<>();
        }
        externalSystemIdMap.put(systemName, systemId);
        return this;
    }

    public CDLAccountSeedBuilder withDuns(String duns) {
        this.duns = duns;
        return this;
    }

    public CDLAccountSeedBuilder withDomainCountries(Set<Pair<String, String>> domainCountries) {
        this.domainCountries = domainCountries;
        return this;
    }

    public CDLAccountSeedBuilder addDomainCountryPair(Pair<String, String> pair) {
        return addDomainCountryPair(pair.getLeft(), pair.getRight());
    }

    public CDLAccountSeedBuilder addDomainCountryPair(String domain, String country) {
        Preconditions.checkArgument(domain != null || country != null);
        if (domainCountries == null) {
            domainCountries = new HashSet<>();
        }
        domainCountries.add(Pair.of(domain, country));
        return this;
    }

    public CDLAccountSeedBuilder withNameCountries(Set<Pair<String, String>> nameCountries) {
        this.nameCountries = nameCountries;
        return this;
    }

    public CDLAccountSeedBuilder addNameCountryPair(Pair<String, String> pair) {
        return addNameCountryPair(pair.getLeft(), pair.getRight());
    }

    public CDLAccountSeedBuilder addNameCountryPair(String name, String country) {
        Preconditions.checkArgument(name != null || country != null);
        if (nameCountries == null) {
            nameCountries = new HashSet<>();
        }
        nameCountries.add(Pair.of(name, country));
        return this;
    }

    public CDLAccountSeed build() {
        return new CDLAccountSeed(id, latticeAccountId, externalSystemIdMap, duns, domainCountries, nameCountries);
    }
}
