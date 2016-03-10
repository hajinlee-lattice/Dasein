var mod = angular.module('mainApp.core.services.FeatureFlagService', [
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.BrowserStorageUtility'
]);

mod.service('FeatureFlagService', function ($q, $http, BrowserStorageUtility, RightsUtility) {

    this.GetAllFlags = function() {
        var deferred = $q.defer();
        GetAllFlagsAsync(deferred);
        return deferred.promise;
    };

    this.FlagIsEnabled = GetFlag;

    // =======================================================
    // flag schema/hash ==> must in sync with backend schema
    // =======================================================
    var flags = {
        CHANGE_MODEL_NAME: 'ChangeModelNames',
        DELETE_MODEL: 'DeleteModels',
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

        JOBS_PAGE: 'JobsPage',
        MARKETO_SETTINGS_PAGE: 'MarketoSettingsPage',
    };
    this.Flags = function(){ return flags; };

    var flagValues = {};

    function GetAllFlagsAsync(promise) {
        // feature flag cached
        if (flagValues.length > 0) {
            promise.resolve(flagValues);
            return;
        }

        // retrieve feature flag
        var sessionDoc = BrowserStorageUtility.getClientSession();
        if (sessionDoc === null || !sessionDoc.hasOwnProperty("Tenant")) {
            promise.resolve({}); // should not attempt to get flags before logging in a tenant
            return;
        }

        var tenantId = sessionDoc.Tenant.Identifier;

        $http({
            method: 'GET',
            url: '/pls/config/featureflags?tenantId=' + tenantId
        })
        .success(function(data) {
            for(var key in data) {
                flagValues[key] = data[key];
            }

            // update user-level flags
            UpdateFlagsBasedOnRights();

            promise.resolve(flagValues);
        })
        .error(function() {
            // if cannot get feature flags from backend
            SetFlag(flags.ADMIN_ALERTS_TAB, false);
            SetFlag(flags.SETUP_PAGE, false);
            SetFlag(flags.ACTIVATE_MODEL_PAGE, false);
            SetFlag(flags.SYSTEM_SETUP_PAGE, false);
            SetFlag(flags.DEPLOYMENT_WIZARD_PAGE, false);
            SetFlag(flags.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE, false);
            SetFlag(flags.LEAD_ENRICHMENT_PAGE, false);

            // update user-level flags
            UpdateFlagsBasedOnRights();

            promise.resolve(flagValues);
        });
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
        UpdateFlag(flags.CHANGE_MODEL_NAME, RightsUtility.currentUserMay("Edit", "Models"));
        UpdateFlag(flags.DELETE_MODEL, RightsUtility.currentUserMay("Edit", "Models"));
        UpdateFlag(flags.UPLOAD_JSON, RightsUtility.currentUserMay("Create", "Models"));

        UpdateFlag(flags.USER_MGMT_PAGE, RightsUtility.currentUserMay("View", "Users"));
        UpdateFlag(flags.ADD_USER, RightsUtility.currentUserMay("Edit", "Users"));
        UpdateFlag(flags.CHANGE_USER_ACCESS, RightsUtility.currentUserMay("Edit", "Users"));
        UpdateFlag(flags.DELETE_USER, RightsUtility.currentUserMay("Edit", "Users"));

        UpdateFlag(flags.ADMIN_PAGE, RightsUtility.currentUserMay("View", "Reporting"));
        UpdateFlag(flags.MODEL_HISTORY_PAGE, RightsUtility.currentUserMay("View", "Reporting"));
        UpdateFlag(flags.SYSTEM_SETUP_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));
        UpdateFlag(flags.ACTIVATE_MODEL_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));

        UpdateFlag(flags.SETUP_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));
        UpdateFlag(flags.DEPLOYMENT_WIZARD_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));
        UpdateFlag(flags.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));
        UpdateFlag(flags.LEAD_ENRICHMENT_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));

        UpdateFlag(flags.JOBS_PAGE, RightsUtility.currentUserMay("View", "Jobs"));
        UpdateFlag(flags.MARKETO_SETTINGS_PAGE, RightsUtility.currentUserMay("Edit", "Configurations"));
    }

});