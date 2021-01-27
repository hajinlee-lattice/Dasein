package com.latticeengines.domain.exposed.metadata;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public enum InterfaceName {
    Id, //
    InternalId, //
    DUNS, // mostly as match input/key
    DunsNumber, // mostly as match result
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
    Email, SecondaryEmail, OtherEmail, //
    Salutation, //
    BirthDate, //
    DoNotMail, //
    ContactName, //

    City, //
    State, //
    PostalCode, //
    Country, //
    PhoneNumber, SecondaryPhoneNumber, OtherPhoneNumber, //
    Address_Street_1, //
    Address_Street_2, ContactCity, ContactState, ContactCountry, ContactPostalCode, //
    Contact_Address_Street_1, Contact_Address_Street_2, //
    PrimaryMobileDeviceID, SecondaryMobileDeviceID, OtherMobileDeviceID, //

    Website, //

    CompanyName, //
    Industry, //
    LeadSource, //
    IsClosed, //
    StageName, //
    StageNameId, //
    AnnualRevenue, //
    NumberOfEmployees, //
    YearStarted, //

    TransactionType, //
    TransactionTime, //
    TransactionDate, //
    TransactionDayPeriod, //
    __StreamDateId, // legacy partition key
    StreamDateId, __StreamDate, //
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

    TimeRanges, // [start time,end time] tuple

    // These values are for Curated Attributes.
    NumberOfContacts, EntityLastUpdatedDate, EntityCreatedDate, EntityCreatedSource, EntityCreatedType, EntityCreatedSystemType,

    // CDL External
    SalesforceSandboxAccountID, SalesforceSandboxContactID, MarketoAccountID, EloquaAccountID,

    // Data Cloud
    LatticeAccountId, // Id in AccountMaster
    IsMatched, LDC_Name,

    // Rating
    ModelId, Rating, Score, RawScore, ExpectedValue, Likelihood, Lift,

    // WebVisit
    WebVisitPageUrl, UserId, WebVisitDate, SourceMedium, SourceMediumId, WebVisitProfile, //
    UtmSource, UtmMedium, UtmCampaign, UtmTerm, UtmContent, //

    // WebVisitPathPattern
    PathPatternName, PathPattern, PathPatternId,

    // Eloqua Activity
    ActivityType,

    // Marketing Activity
    ActivityDate, ActivityTypeId,

    OpportunityId,

    ModelName, ModelNameId, HasIntent, IntentScore, BuyingScore, //

    // Derived Attributes
    DerivedId, DerivedName, DerivedPattern,

    // timeline column
    Detail2, Detail1, EventTimestamp, EventType, StreamType, Source,

    // activity alert
    AlertData, AlertName, CreationTimestamp, AlertCategory, //

    PartitionKey, SortKey,

    // DCP
    RegistrationNumber,

    // timelineExport
    DomesticUltimateDuns, GlobalUltimateDuns, IsPrimaryDomain, EventDate, Count,

    // Internal
    __Row_Count__, // total row count in activity store aggregation
    __Composite_Key__, // primary key for internal use

    //DataVison
    GCA_ID;

    private static final Set<String> KeyIds = ImmutableSet.of( //
            InterfaceName.EntityId.name(), //
            InterfaceName.AccountId.name(), //
            InterfaceName.ContactId.name() //
    );

    private static final Set<String> KeyIdsUC = ImmutableSet.of( //
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
    public static boolean isKeyId(String id, boolean caseSensitive) {
        if (caseSensitive) {
            // Strip out the prefix, if it exists
            String[] strs = id.split("__");
            return KeyIds.contains(strs[strs.length - 1]);
        } else {
            return id != null && KeyIdsUC.contains(id.toUpperCase());
        }
    }
}
