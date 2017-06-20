package com.latticeengines.domain.exposed.metadata;

public enum InterfaceName {
    Id, //
    InternalId, //
    DUNS, //
    Event, //
    Domain, //

    // IDS - shared between LPI and CDL
    AccountId, //
    ContactId, //
    CategoryId, //
    SubcategoryId, //

    LastModifiedDate, //
    CreatedDate, //

    FirstName, //
    LastName, //
    Title, //
    Email, //
    Salutation, //
    BirthDate, //

    City, //
    State, //
    PostalCode, //
    Country, //
    PhoneNumber, //

    Website, //

    CompanyName, //
    Industry, //
    LeadSource, //
    IsClosed, //
    StageName, //
    AnnualRevenue, //
    NumberOfEmployees, //
    YearStarted, //
    
    Quantity, //
    Amount,

    // CDL
    VdbAccountId, // account id whose uniqueness is managed by vdb
    LEAccountIDLong, // TODO: to be replaced by VdbAccountId

    // Data Cloud
    LatticeAccountId // Id in AccountMaster
}
