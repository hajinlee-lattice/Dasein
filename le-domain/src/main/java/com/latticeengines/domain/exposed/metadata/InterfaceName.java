package com.latticeengines.domain.exposed.metadata;

public enum InterfaceName {
    Id, //
    InternalId, //
    DUNS, //
    Event, //
    Domain, //
    AccountId, // reused in cdl to be unique in one customer universe
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
