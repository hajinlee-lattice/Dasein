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
    Name, //
    Title, //
    Email, //
    Salutation, //
    BirthDate, //
    DoNotMail, //
    ContactName, //

    City, //
    State, //
    PostalCode, //
    Country, //
    PhoneNumber, //
    Address_Street_1, //

    Website, //

    CompanyName, //
    Industry, //
    LeadSource, //
    IsClosed, //
    StageName, //
    AnnualRevenue, //
    NumberOfEmployees, //
    YearStarted, //

    TransactionType, //
    TransactionTime, //
    TransactionDate, //
    ProductName, //
    Quantity, //
    TotalQuantity, //
    Amount, //
    TotalAmount, //
    OrderId, //

    // CDL
    VdbAccountId, // account id whose uniqueness is managed by vdb
    LEAccountIDLong, // TODO: to be replaced by VdbAccountId
    VdbContactId, // contact id whose uniqueness is managed by vdb
    LEContactIDLong, // TODO: to be replaced by VdbContactId
    External_ID, // TODO: temporarily used for contact Id before we have real
                 // contact data
    SalesforceAccountID, // salesforce account ID

    // Data Cloud
    LatticeAccountId, // Id in AccountMaster
    LDC_Name,

    // Internal
    __Composite_Key__ // primary key for internal use
}
