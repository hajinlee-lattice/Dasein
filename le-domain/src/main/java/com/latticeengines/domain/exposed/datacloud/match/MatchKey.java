package com.latticeengines.domain.exposed.datacloud.match;

public enum MatchKey {
    ExternalId, //
    Domain, // anything can be parsed to domain, email, website, etc.
    Email, //
    Name, //
    City, //
    State, //
    Country, // default to USA
    Zipcode, //
    PhoneNumber, //
    DUNS, //
    LatticeAccountID; // internal id for quicker lookup in curated AccountMaster

}
