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
    CRMId, //
    ProductId, //
    TransactionId, //

    LastModifiedDate, //
    CreatedDate, //

    FirstName, //
    LastName, //
    Title, //
    Email, //
    Salutation, //
    BirthDate, //
    DoNotMail, //

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
    VdbContactId, // contact id whose uniqueness is managed by vdb
    LEContactIDLong, // TODO: to be replaced by VdbContactId
    External_ID, // TODO: temporarily used for contact Id before we have real
                 // contact data
    SalesforceAccountID, // salesforce account ID

    // Data Cloud
    LatticeAccountId // Id in AccountMaster

}
