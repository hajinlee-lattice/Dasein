package com.latticeengines.domain.exposed.metadata;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public enum InterfaceName {
    Id, //
    InternalId, //
    DUNS, //
    Event, //
    Domain, //

    // IDS - shared between LPI and CDL
    EntityId, // Represents the ID known as CDL ID internally.
    AccountId, //
    ContactId, //
    CategoryId, //
    SubcategoryId, //
    ProductId, //
    ProductType, //
    ProductBundle, //
    ProductName, //
    ProductLine, //
    ProductFamily, //
    ProductCategory, //
    Description, //
    ProductStatus, //
    ProductBundleId, //
    ProductLineId, //
    ProductFamilyId, //
    ProductCategoryId, //
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
    LEAccount_ID, //
    Period_ID, //
    Train, //
    AverageScore, //

    LastModifiedDate, //
    CreatedDate, //
    LastActivityDate, //

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
    StageNameId,//
    AnnualRevenue, //
    NumberOfEmployees, //
    YearStarted, //

    TransactionType, //
    TransactionTime, //
    TransactionDate, //
    TransactionDayPeriod, //
    __StreamDateId, //
    __StreamDate, //
    TransactionCount, //
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
    Longitude, Latitude,

    // CDL
    AtlasLookupKey, //
    VdbAccountId, // account id whose uniqueness is managed by vdb
    LEAccountIDLong, // TODO: to be replaced by VdbAccountId
    VdbContactId, // contact id whose uniqueness is managed by vdb
    LEContactIDLong, // TODO: to be replaced by VdbContactId
    External_ID, // TODO: temporarily used for contact Id before we have real
                 // contact data
    SalesforceAccountID, // salesforce account ID
    SalesforceContactID, // salesforce contact ID
    // AccountId from customer when entity match is enabled
    CustomerAccountId, //
    // ContactId from customer when entity match is enabled
    CustomerContactId, //

    CDLCreatedTime, // creation timestamp of CDL entities
    CDLUpdatedTime, // update timestamp of CDL entities
    LatticeExportTime, // timestamp for csv export
    CDLBatchSource, // indicator if the source contribute the system batch

    CDLTemplateName, CDLCreatedTemplate,
                       // store

    ConsolidateReport, //

    Type, //
    AnnualRevenueCurrency, SpendAnalyticsSegment, RepresentativeAccounts, CustomerParentAccountID, DoNotCall, LeadStatus, LeadType, Cost, TotalCost, //

    // These values are for Curated Attributes.
    NumberOfContacts, EntityLastUpdatedDate, EntityCreatedDate, EntityCreatedSource, EntityCreatedType,

    // CDL External
    SalesforceSandboxAccountID, SalesforceSandboxContactID, MarketoAccountID, EloquaAccountID,

    // Data Cloud
    LatticeAccountId, // Id in AccountMaster
    IsMatched, LDC_Name,

    // Rating
    ModelId, Rating, Score, RawScore, ExpectedValue, Likelihood, Lift,

    // WebVisit
    WebVisitPageUrl, UserId, WebVisitDate, SourceMedium, SourceMediumId, WebVisitProfile, //

    // WebVisitPathPattern
    PathPatternName, PathPattern, PathPatternId,

    // Eloqua Activity
    ActivityType,

    //Marketing Activity
    ActivityDate, ActivityTypeId,

    OpportunityId,

    // Internal
    __Row_Count__, // total row count in activity store aggregation
    __Composite_Key__; // primary key for internal use

    private static final Set<String> EntityIds = ImmutableSet.of( //
            InterfaceName.EntityId.name(), //
            InterfaceName.AccountId.name(), //
            InterfaceName.ContactId.name() //
    );

    private static final Set<String> EntityIdsUC = ImmutableSet.of( //
            InterfaceName.EntityId.name().toUpperCase(), //
            InterfaceName.AccountId.name().toUpperCase(), //
            InterfaceName.ContactId.name().toUpperCase() //
    );

    /**
     * Whether it is reserved field for internal entity ID
     *
     * @param id
     * @return
     */
    public static boolean isEntityId(String id, boolean caseSensitive) {
        if (caseSensitive) {
            return EntityIds.contains(id);
        } else {
            return id != null && EntityIdsUC.contains(id.toUpperCase());
        }
    }
}
