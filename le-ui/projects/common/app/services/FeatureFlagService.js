var mod = angular.module('mainApp.core.services.FeatureFlagService', [
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.BrowserStorageUtility'
]);

mod.service('FeatureFlagService', function ($q, $http, BrowserStorageUtility, RightsUtility) {

    this.GetAllFlags = function(ApiHost) {
        console.log('!! GetAllFlags()', ApiHost);
        var deferred = $q.defer();
        GetAllFlagsAsync(deferred, ApiHost);
        return deferred.promise;
    };

    this.FlagIsEnabled = GetFlag;

    this.UserIs = function(levels){
        var sessionDoc = BrowserStorageUtility.getClientSession(),
            levels = levels || '',
            levelsAr = levels.split(',');

        if(levelsAr.includes(sessionDoc.AccessLevel)) {
            return true;
        }
        return false;
    }

    // =======================================================
    // flag schema/hash ==> must in sync with backend schema
    // =======================================================
    var flags = {
        CHANGE_MODEL_NAME: 'ChangeModelNames',
        DELETE_MODEL: 'DeleteModels',

        REVIEW_MODEL: 'ReviewModel',
        UPLOAD_JSON: 'UploadSummaryJson',

        USER_MGMT_PAGE: 'UserManagementPage',
        ADD_USER: 'AddUsers',
        CHANGE_USER_ACCESS: 'ChangeUserAccess',
        DELETE_USER: 'DeleteUsers',

        ADMIN_PAGE: 'AdminPage',
        ADMIN_ALERTS_TAB: 'AdminAlertsTab',

        MODEL_HISTORY_PAGE: 'ModelCreationHistoryPage',
        SYSTEM_SETUP_PAGE: 'SystemSetupPage',
        ACTIVATE_MODEL_PAGE: 'ActivateModelPage',

        SETUP_PAGE: 'SetupPage',
        DEPLOYMENT_WIZARD_PAGE: 'DeploymentWizardPage',
        REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE: 'RedirectToDeploymentWizardPage',
        LEAD_ENRICHMENT_PAGE: 'LeadEnrichmentPage',

        CAMPAIGNS_PAGE: 'EnableCampaignUI',
        JOBS_PAGE: 'JobsPage',
        MARKETO_SETTINGS_PAGE: 'MarketoSettingsPage',
        API_CONSOLE_PAGE: 'APIConsolePage',
        LATTICE_MARKETO_PAGE: 'EnableLatticeMarketoCredentialPage',

        ALLOW_PIVOT_FILE:'AllowPivotFile',
        USE_ELOQUA_SETTINGS: 'UseEloquaSettings',
        USE_MARKETO_SETTINGS: 'UseMarketoSettings',
        USE_SALESFORCE_SETTINGS: 'UseSalesforceSettings',

        ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES: 'EnableInternalEnrichmentAttributes',
        ENABLE_DATA_PROFILING_V2: 'EnableDataProfilingV2',
        ENABLE_FUZZY_MATCH: 'EnableFuzzyMatch',
        ENABLE_CDL: 'EnableCdl',

        LATTICE_INSIGHTS: 'LatticeInsights'
    };

    this.Flags = function(){ return flags; };

    var flagValues = {};

    function GetAllFlagsAsync(promise, ApiHost) {
        console.log('!! GetAllFlagsAsync();', promise, ApiHost);
        // feature flag cached
        if (Object.keys(flagValues).length > 0) {
            console.log('!! GetAllFlagsAsync(); A', Object.keys(flagValues).length, Object.keys(flagValues).length > 0, flagValues);
            promise.resolve(flagValues);
            return;
        }
        
        var url = (ApiHost == '/ulysses' ? ApiHost + '/tenant' : '/pls' + '/config') + '/featureflags';
        
        // retrieve feature flag
        if (ApiHost != '/ulysses') {
            var sessionDoc = BrowserStorageUtility.getClientSession();
            
            if (sessionDoc === null || !sessionDoc.hasOwnProperty("Tenant")) {
                console.log('!! GetAllFlagsAsync(); B', sessionDoc.hasOwnProperty("Tenant"), sessionDoc);
                promise.resolve({}); // should not attempt to get flags before logging in a tenant
                return;
            }
        
            var tenantId = sessionDoc.Tenant.Identifier;
            url += '?tenantId=' + tenantId;
        }

        console.log('!! GetAllFlagsAsync(); C', url, tenantId);
        $http({
            method: 'GET',
            url: url
        }).success(function(data) {
            console.log('!! GetAllFlagsAsync(); D SUCCESS', data);
            for(var key in data) {
                console.log('!! GetAllFlagsAsync(); E', key, data[key]);
                flagValues[key] = data[key];
            }

            // update user-level flags
            UpdateFlagsBasedOnRights();

            console.log('!! GetAllFlagsAsync(); F', flagValues);
            promise.resolve(flagValues);
        }).error(function() {
            console.log('!! GetAllFlagsAsync(); G ERROR');
            // if cannot get feature flags from backend
            SetFlag(flags.ADMIN_ALERTS_TAB, false);
            SetFlag(flags.ALLOW_PIVOT_FILE, false);
            SetFlag(flags.SETUP_PAGE, false);
            SetFlag(flags.ACTIVATE_MODEL_PAGE, false);
            SetFlag(flags.SYSTEM_SETUP_PAGE, false);
            SetFlag(flags.DEPLOYMENT_WIZARD_PAGE, false);
            SetFlag(flags.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE, false);
            SetFlag(flags.LEAD_ENRICHMENT_PAGE, false);
            SetFlag(flags.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES, false);

            // update user-level flags
            UpdateFlagsBasedOnRights();

            console.log('!! GetAllFlagsAsync(); H ERROR', flagValues);
            promise.resolve(flagValues);
        });
        console.log('!! GetAllFlagsAsync(); I END');
    }

    function SetFlag(flag, value) { flagValues[flag] = value; }
    function UpdateFlag(flag, value) { SetFlagUsingBooleanAnd(flag, value); }
    function SetFlagUsingBooleanAnd(flag, value) {
        if (flagValues.hasOwnProperty(flag)) {
            SetFlag(flag, flagValues[flag] && value);
        } else {
            SetFlag(flag, value);
        }
    }

    /**
     * @return {boolean}
     */
    function GetFlag(flag) { return flagValues[flag] || false; }

    function UpdateFlagsBasedOnRights() {

        UpdateFlag(flags.VIEW_SAMPLE_LEADS, RightsUtility.currentUserMay("View", "Sample_Leads"));
        UpdateFlag(flags.VIEW_REFINE_CLONE, RightsUtility.currentUserMay("View", "Refine_Clone"));
        UpdateFlag(flags.EDIT_REFINE_CLONE, RightsUtility.currentUserMay("Edit", "Refine_Clone"));

        UpdateFlag(flags.CHANGE_MODEL_NAME, RightsUtility.currentUserMay("Edit", "Models"));
        UpdateFlag(flags.DELETE_MODEL, RightsUtility.currentUserMay("Edit", "Models"));
        UpdateFlag(flags.REVIEW_MODEL, false);
        UpdateFlag(flags.UPLOAD_JSON, RightsUtility.currentUserMay("Create", "Models"));

        UpdateFlag(flags.USER_MGMT_PAGE, RightsUtility.currentUserMay("View", "Users"));
        UpdateFlag(flags.ADD_USER, RightsUtility.currentUserMay("Edit", "Users"));
        UpdateFlag(flags.CHANGE_USER_ACCESS, RightsUtility.currentUserMay("Edit", "Users"));
        UpdateFlag(flags.DELETE_USER, RightsUtility.currentUserMay("Edit", "Users"));

        UpdateFlag(flags.ADMIN_PAGE, RightsUtility.currentUserMay("View", "Reporting"));
        UpdateFlag(flags.ADMIN_ALERTS_TAB, RightsUtility.currentUserMay("Edit", "Configurations"));
        UpdateFlag(flags.MODEL_HISTORY_PAGE, RightsUtility.currentUserMay("View", "Reporting"));
        UpdateFlag(flags.SYSTEM_SETUP_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));
        UpdateFlag(flags.ACTIVATE_MODEL_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));

        UpdateFlag(flags.SETUP_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));
        UpdateFlag(flags.DEPLOYMENT_WIZARD_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));
        UpdateFlag(flags.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));
        UpdateFlag(flags.LEAD_ENRICHMENT_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));

        UpdateFlag(flags.JOBS_PAGE, RightsUtility.currentUserMay("View", "Jobs"));
        UpdateFlag(flags.MARKETO_SETTINGS_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));
        UpdateFlag(flags.API_CONSOLE_PAGE, RightsUtility.currentUserMay("Create", "Oauth2Token"));
    }

});