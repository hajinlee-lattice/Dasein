package com.latticeengines.domain.exposed.datacloud.match;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public enum MatchKey {
    ExternalId, //
    Domain, // anything can be parsed to domain, email, website, etc.
    Email, //
    Name, //
    City, //
    State, //
    Country, // default to USA for name + location -> duns
    Zipcode, //
    PhoneNumber, //
    DUNS, //
    LookupId, // for CDL lookup, can be AccountId or one of the external lookup
              // ids
    LatticeAccountID, // internal id for quicker lookup in curated AccountMaster
    SystemId, // Use one MatchKey for all system IDs, eg. (user provided) AccountId, or external ID such as SfdcId,
              // MktoId, etc.
    EntityId, // for entity match, internal id for quicker lookup in entity seed
    PreferredEntityId; // preferred ID if we need to allocate new entity

    public static final Map<MatchKey, String> LDC_MATCH_KEY_STD_FLDS = ImmutableMap.<MatchKey, String> builder()
            .put(Domain, InterfaceName.Website.name()) //
            .put(Name, InterfaceName.CompanyName.name()) //
            .put(City, InterfaceName.City.name()) //
            .put(State, InterfaceName.State.name()) //
            .put(Country, InterfaceName.Country.name()) //
            .put(Zipcode, InterfaceName.PostalCode.name()) //
            .put(PhoneNumber, InterfaceName.PhoneNumber.name()) //
            .put(DUNS, InterfaceName.DUNS.name()) //
            .build();
}
