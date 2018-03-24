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
    ProductId, //
    ProductType, //
    ProductBundle, //
    TransactionId, //
    PeriodId, //
    PeriodName, //
    Target, //
    __Revenue, //
    NormalizedScore, //
    PredictedRevenue, //
    ExpectedRevenue, //
    Probability, //
    AnalyticPurchaseState_ID, //
    Train, //
    AverageScore, //

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
    Address_Street_2,

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
    TransactionDayPeriod, //
    TransactionCount, //
    ProductName, //
    Quantity, //
    TotalQuantity, //
    Amount, //
    TotalAmount, //
    OrderId, //
    Margin, //
    HasPurchased, //
    ShareOfWallet, //
    SpendChange, //
    TotalSpendOvertime, //
    AvgSpendOvertime, //
    CustomTrxField, //

    // CDL
    VdbAccountId, // account id whose uniqueness is managed by vdb
    LEAccountIDLong, // TODO: to be replaced by VdbAccountId
    VdbContactId, // contact id whose uniqueness is managed by vdb
    LEContactIDLong, // TODO: to be replaced by VdbContactId
    External_ID, // TODO: temporarily used for contact Id before we have real
                 // contact data
    SalesforceAccountID, // salesforce account ID
    SalesforceContactID, // salesforce contact ID

    CDLCreatedTime, // creation timestamp of CDL entities
    CDLUpdatedTime, // update timestamp of CDL entities

    ConsolidateReport, //

    Type, //
    AnnualRevenueCurrency,
    SpendAnalyticsSegment,
    CustomerParentAccountID,
    DoNotCall,
    LeadStatus,
    Cost,
    TotalCost, //

    // CDL External
    SalesforceSandboxAccountID,
    SalesforceSandboxContactID,
    MarketoAccountID,
    EloquaAccountID,

    // Data Cloud
    LatticeAccountId, // Id in AccountMaster
    IsMatched,
    LDC_Name,

    // Rating
    ModelId,
    Rating,
    Score,
    RawScore,
    ExpectedValue,
    Likelihood,
    Lift,

    // Internal
    __Composite_Key__ // primary key for internal use
}
