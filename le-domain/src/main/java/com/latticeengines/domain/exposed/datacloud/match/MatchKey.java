package com.latticeengines.domain.exposed.datacloud.match;

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
}
