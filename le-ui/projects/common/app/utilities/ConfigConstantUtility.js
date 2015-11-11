angular.module('mainApp.appCommon.utilities.ConfigConstantUtility', [])
.service('ConfigConstantUtility', function () {
    // View Keys used for Navigation
    this.ViewAccount = "VIEW_ACCOUNT";
    this.ViewActionItems = "VIEW_ACTION_ITEMS";
    this.ViewAdmin = "VIEW_ADMIN";
    this.ViewBard = "VIEW_LEAD_SCORING";
    this.ViewMessages = "VIEW_INVITATION";
    this.ViewPlay = "VIEW_PLAY";
    this.ViewSales = "VIEW_SALES";
    this.ViewDashboardCenter = "VIEW_DASHBOARDCENTER";
    this.ViewChangePassword = "VIEW_CHANGE_PASSWORD";
    this.ViewProductHierarchy = "VIEW_PRODUCTHIERARCHY";

    // Hashes used for navigation
    // salesPRISM hashes
    this.ActionCenterHash = "#ActionCenter";
    this.SalesListHash = "#SalesList";
    this.PlayListHash = "#PlayList";
    this.LaunchMetricsHash = "#LaunchMetrics";
    this.LaunchRulesHash = "#LaunchRules";
    this.LaunchFiltersHash = "#LaunchFilters";
    this.PlayQuestionsHash = "#PlayQuestions";
    this.HoldoutsHash = "#Holdouts";
    this.ConfigConsoleHash = "#ConfigConsole";
    this.AdminConsoleHash = "#AdminConsole";
    this.UsersHash = "#Users";
    this.UserGroupsHash = "#UserGroups";
    this.AlertsHash = "#Alerts";
    this.PreviewAlertsHash = "#PreviewAlerts";
    this.FileTemplatesHash = "#FileTemplates";
    this.MailTemplatesHash = "#MailTemplates";
    this.FileUploadHash = "#FileUpload";
    this.ExternalDataHash = "#ExternalData";
    this.InvitationsHash = "#Invitations";
    this.ProductHierarchyHash = "#ProductHierarchy";

    // PLS hashes
    this.BardStatusHash = "#BardStatus";
    this.BardConfigHash = "#BardConfig";
    this.BardReportHash = "#BardReport";
    this.BardModelHash = "#BardModel";

    // Dante hashes
    this.DantePlayHash = "#DantePlays";
    this.DantePurchaseTrendsHash = "#DantePurchaseTrends";
    this.DanteCompanyDetailsHash = "#DanteCompanyDetails";
    this.DanteContactsHash = "#DanteContacts";


    // Right Keys used for Permission
    this.Play = "Play";
    this.ApprovePlay = "Approve_Play";

    // Currently Supported Error Code Descriptions
    this.DetailedDescriptionDataKey = "DetailedDescription";
    this.ImpersonationWriteError = "IMPERSONATION_WRITE_ERROR";
    this.ValidationInProgressError = "VALIDATION_ALREADY_IN_PROGRESS";
    this.ModelScoreErrorTargetFilterNoTargets = "MODELSCORE_TARGET_FILTERS_NO_ACCOUNTS";
    this.ModelScoreErrorTrainingFilterNoTargets = "MODELSCORE_TRAINING_FILTERS_NO_ACCOUNTS";

    // Online Help
    this.SalesHelp = "/Help/Sales_Help/index.html";
    this.PlayHelp = "/Help/PlayMaker_Help/index.html";
    this.AdminHelp = "/Help/Admin_Help/index.html";

    // ScoreSourceID
    this.ScoreSourceAnalytic = "Analytics";
    this.ScoreSourceUploaded = "Uploaded";
    this.ScoreSourceRuleBased = "RuleBased";
    this.ScoreSourceUnspecified = "Unspecified";

    // Field Length Validations
    this.StringShort = 150;
    this.StringInternalName = 50;
    // if we ever add maxRequestLength in web.config, please change this to match
    this.FileUploadMaxBytes = 4194304; // 4MB (default for .aspx)
    this.NumericFieldMaxLength = 9;
    this.MonetaryFieldMaxLength = 12;

    // PlayStage enum values
    this.PlayStageDetails = "Details";
    this.PlayStageTargets = "Targets";
    this.PlayStageRules = "Rules";
    this.PlayStageScore = "Score";
    this.PlayStageContent = "Content";
    this.PlayStageComplete = "Complete";

    // PlayScore enum values
    this.PlayScoreNone = "None";
    this.PlayScoreError = "Error";
    this.PlayScoreCanceled = "Canceled";
    this.PlayScoreProcessing = "Processing";
    this.PlayScoreComplete = "Complete";

    // Target Filter Relation values
    this.Contains = "CONTAINS";
    this.Equal = "EQUAL";
    this.GreaterThan = "GREATER_THAN";
    this.GreaterOrEqual = "GREATER_OR_EQUAL";
    this.LessThan = "LESS_THAN";
    this.LessOrEqual = "LESS_OR_EQUAL";
    this.GreaterThanSymbol = ">";
    this.GreaterOrEqualSymbol = "≥";
    this.LessThanSymbol = "<";
    this.LessOrEqualSymbol = "≤";
    this.NotEqual = "NOT_EQUAL";
    this.StartsWith = "STARTS_WITH";
    this.AfterNowOffsetMinutes = "AFTER_NOW_OFFSET_MINUTES";
    this.BeforeNowOffsetMinutes = "BEFORE_NOW_OFFSET_MINUTES";
    this.Ever = "EVER";
    this.Before = "BEFORE";
    this.After = "AFTER";
    this.Between = "BETWEEN";
    this.InCurrentPeriod = "IN_CURRENT_PERIOD";
    this.InSamePeriod = "IN_SAME_PERIOD";
    this.InNextPeriod = "IN_NEXT_PERIOD";
    this.Sum = "Sum";
    this.Each = "Each";
    this.AtLeastOnce = "AtLeastOnce";
    this.Average = "Average";
    this.HavePurchased = "HavePurchased";
    this.HaveNotPurchased = "HaveNotPurchased";
    this.Spent = "Spent";
    this.Units = "Units";
    this.Revenue = "Revenue";
    this.IsNull = "IS_NULL";
    this.IsNotNull = "IS_NOT_NULL";
    this.IsEmpty = "IS_EMPTY";
    this.IsNotEmpty = "IS_NOT_EMPTY";

    // Filter Target Type
    this.FilterTargetTypeStandard = "STANDARD";
    this.FilterTargetTypeProductPurchase = "PRODUCT_PURCHASE";

    // Filter Category Type
    this.FilterCategoryStatic = "STATIC";
    FilterCategoryTimesSeries = "TIMES_SERIES";

    // Target Build Type
    this.TargetBuildTypeNone = "None";
    this.TargetBuildTypeBuilder = "Builder";
    this.TargetBuildTypeUpload = "Upload";

    // Upload Algorithms
    this.UploadAccountAndProbabilityList = "UploadAccountAndProbabilityList";
    this.UploadAccountList = "UploadAccountList";
    this.UploadHoldoutList = "UploadHoldoutList";
    this.UploadPlayAccountProducts = "UploadPlayAccountProducts";

    // Upload Managers
    this.FileUploadManagerPlay = "PlayAccountList";
    this.FileUploadManagerPlayAccountProducts = "PlayAccountProducts";

    // TrainingBuildType
    this.TrainingBuildTypeCopyTargetFilter = "CopyTargetFilter";
    this.TrainingBuildTypeExplicitlyDefined = "ExplicitlyDefined";
    this.TrainingBuildTypeEmpty = "Empty";

    // EventBuildType
    this.EventBuildTypeExplicitlyDefined = "ExplicitlyDefined";
    EventBuildTypeDefaultFilter = "DefaultFilter";

    //Launch Rule Type
    this.PlaysWithExistingLeads = "PlaysWithExistingLeads";

    //Launch Destinations
    this.LeadLaunchDestination = {
        SalesPrism: "salesprism",
        Salesforce: "salesforce",
        SalesPrismAndCRM: "salesprism_and_CRM"
    };

    // Export Constants
    this.ExportPlayCustomerList = "PlayCustomerListExport";
    this.ExportPlayCustomerListContacts = "PlayCustomerListContactsExport";
    this.ExportPlayCustomerListWithContacts = "PlayCustomerListWithContactsExport";
    this.ExportHoldoutAccounts = "HoldoutAccountsExport";

    // PriorityType enum
    // These strings defines LikelihoodBuckets[i].ScoringMethodID.
    // And corresponds to LECore enum LeadScoringSourceType in PlayData.cs
    this.LikelihoodScoringMethodProbability = "Probability";
    this.LikelihoodScoringMethodLift = "Lift";
    this.LikelihoodScoringMethodManual = "Manual";
    this.LikelihoodScoringMethodNumValues = "NumValues";

    this.ScoringMethodSuffixLift = "x";
    this.ScoringMethodSuffixProbability = "%";

    // Model Health Score Categories =
    this.ModelHealthScoreExcellent = "EXCELLENT";
    this.ModelHealthScoreGood = "GOOD";
    this.ModelHealthScoreAverage = "AVERAGE";
    this.ModelHealthScoreError = "ERROR";

    this.ModelStatusActive = "Active";
    this.ModelStatusInactive = "Inactive";

    // ComplexFilterTypes:
    this.ComplexFilterType = {
        Training: "TRAINING",
        Target: "TARGET",
        Event: "EVENT",
        RuleBased: "RULE_BASED"
    };

    // Change Password service methods
    this.SalesprismChangePasswordMethod = "./WebLEApplicationServiceHost.svc/RegisterPendingLogAnchorIDs";
    this.SalesprismPasswordEncryptionType = "rsa";
    this.BardChangePasswordMethod = "./BardService.svc/ChangePassword";
    this.BardChangePasswordUponLoginMethod = "./LoginService.svc/ChangePasswordUponLogin";

    // Encryption algorthims
    this.SHA256 = "SHA-256";
    this.Base64 = "Base64";
    this.RSA = "rsa";

    // RSA public key
    this.rsa = {
        n: "BEB87612E3BBEFCFE80F58578A54D58DA46EB5B34D15D5F1B66F0C6040467D9910B434876F4730EAC92A9618C1B9B69CAFB3C4CA6ABC93ACEB018D8A8FA90E53301E6DA68A2D0E059654F0605769E60E7B366990AB55E65B7489C7A79A38F3CE0ECF26DF51401C16702D5811D354F1ED64B22919EFBE67332022C1AE75FDB5CFDE0CB45761AB9D26AFA301581CF06F76BF99CCF466B98836E6752E162FAEE550BE9ADCE5125063F9FAF37ECB5E206A830CE64B97A362260EF99582B4B1D3A3A2334261EDA281C112B478C32E609EDA9408BC3AD518F7F49C71236947D9F7677FD62655958349894AFD079595A7DC0D835A23D6F06A995C17EA254DE0961710B1",
        e: "10001"
    };

    // Corresponds to RuntimeEnums.cs: GeneralCRMInterfaceNames
    this.CrmType = {
        None: "None",
        Salesforce: "Salesforce",
        Siebel: "Siebel",
        Oracle: "Oracle"
    };
});