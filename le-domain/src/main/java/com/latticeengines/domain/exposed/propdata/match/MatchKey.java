package com.latticeengines.domain.exposed.propdata.match;

public enum MatchKey {
    Domain, // anything can be parsed to domain, email, website, etc.

    Name,

    City,
    State,
    Country,  // default to USA

    DUNS,

    LatticeAccountID; // internal id for quicker lookup in curated AccountMaster

}
