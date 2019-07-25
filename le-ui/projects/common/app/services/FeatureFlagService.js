import { actions } from "./featureFlags.redux";

angular
	.module("common.services.featureflag", [
		"mainApp.core.utilities.RightsUtility",
		"common.utilities.browserstorage"
	])
	.service("FeatureFlagService", function(
		$q,
		$http,
		BrowserStorageUtility,
		RightsUtility
	) {
		this.GetAllFlags = function(ApiHost) {
			var deferred = $q.defer();
			GetAllFlagsAsync(deferred, ApiHost);
			return deferred.promise;
		};

		this.FlagIsEnabled = GetFlag;

		/**
		 * @return {boolean}
		 */
		this.UserIs = function(levels) {
			var sessionDoc = BrowserStorageUtility.getClientSession(),
				levels = levels || "",
				levelsAr = levels.split(",");
			return levelsAr.includes(sessionDoc.AccessLevel);
		};

		var products = {
			CG: "Customer Growth"
		};

		// =======================================================
		// flag schema/hash ==> must in sync with backend schema
		// =======================================================
		var flags = {
			// ===================================
			// BEGIN: flags governed by user level
			// ===================================
			VIEW_SAMPLE_LEADS: "View_Sample_Leads",
			VIEW_REFINE_CLONE: "View_Refine_Clone",
			EDIT_REFINE_CLONE: "Edit_Refine_Clone",
			VIEW_REMODEL: "View_Remodel",
			CHANGE_MODEL_NAME: "ChangeModelNames",
			DELETE_MODEL: "DeleteModels",
			REVIEW_MODEL: "ReviewModel",
			UPLOAD_JSON: "UploadSummaryJson",
			USER_MGMT_PAGE: "UserManagementPage",
			ADD_USER: "AddUsers",
			CHANGE_USER_ACCESS: "ChangeUserAccess",
			DELETE_USER: "DeleteUsers",
			ADMIN_PAGE: "AdminPage",
			MODEL_HISTORY_PAGE: "ModelCreationHistoryPage",
			JOBS_PAGE: "JobsPage",
			MARKETO_SETTINGS_PAGE: "MarketoSettingsPage",
			API_CONSOLE_PAGE: "APIConsolePage",
			// ===================================
			// END: flags governed by user level
			// ===================================

			// ====================
			// BEGIN: product flags
			// ====================
			// These are actually product flag (whether the customer has purchased the product or not)
			ENABLE_CDL: "EnableCdl",
			ENABLE_ENTITY_MATCH: "EnableEntityMatch",
			ALWAYS_ON_CAMPAIGNS: "AlwaysOnCampaigns",
			// ====================
			// END: product flags
			// ====================

			// ================
			// BEGIN: LP2 flags
			// ================
			ADMIN_ALERTS_TAB: "AdminAlertsTab",
			SETUP_PAGE: "SetupPage",
			DEPLOYMENT_WIZARD_PAGE: "DeploymentWizardPage",
			SYSTEM_SETUP_PAGE: "SystemSetupPage",
			ACTIVATE_MODEL_PAGE: "ActivateModelPage",
			LEAD_ENRICHMENT_PAGE: "LeadEnrichmentPage",
			// ================
			// END: LP2 flags
			// ================

			REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE:
				"RedirectToDeploymentWizardPage",
			ALLOW_PIVOT_FILE: "AllowPivotFile",
			ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES:
				"EnableInternalEnrichmentAttributes",
			LATTICE_INSIGHTS: "LatticeInsights",
			VDB_MIGRATION: "VDBMigration",
			SCORE_EXTERNAL_FILE: "ScoreExternalFile",
			ENABLE_CROSS_SELL_MODELING: "EnableCrossSellModeling",
			ENABLE_FILE_IMPORT: "EnableFileImport",
			ENABLE_PRODUCT_BUNDLE_IMPORT: "EnableProductBundleImport",
			ENABLE_PRODUCT_HIERARCHY_IMPORT: "EnableProductHierarchyImport",
			ENABLE_PRODUCT_PURCHASE_IMPORT: "EnableProductPurchaseImport",
			PLAYBOOK_MODULE: "PlaybookModule",
			LAUNCH_PLAY_TO_MAP_SYSTEM: "LaunchPlayToMapSystem",
			ENABLE_EXTERNAL_INTEGRATION: "EnableExternalIntegration",
			MIGRATION_TENANT: "MigrationTenant",
			ENABLE_MULTI_TEMPLATE_IMPORT: "EnableMultiTemplateImport",
			ALPHA_FEATURE: "AlphaFeature",

			//TODO: deprecated flags
			CAMPAIGNS_PAGE: "EnableCampaignUI",
			USE_ELOQUA_SETTINGS: "UseEloquaSettings",
			USE_MARKETO_SETTINGS: "UseMarketoSettings",
			USE_SALESFORCE_SETTINGS: "UseSalesforceSettings",
			LATTICE_MARKETO_PAGE: "EnableLatticeMarketoCredentialPage",
			ENABLE_FUZZY_MATCH: "EnableFuzzyMatch",
			ENABLE_DATA_PROFILING_V2: "EnableDataProfilingV2"
		};

		this.Flags = function() {
			return flags;
		};

		var flagValues = {};
		var purchasedProducts = [];

		function GetAllFlagsAsync(promise, ApiHost) {
			// feature flag cached
			if (Object.keys(flagValues).length > 0) {
				promise.resolve(flagValues);
				return;
			}

			var url =
				(ApiHost === "/ulysses" ? "/ulysses" : "/pls") +
				"/tenantconfig";

			$http({
				method: "GET",
				url: url
			})
				.success(function(data) {
					for (var key in data["FeatureFlags"]) {
						flagValues[key] = data["FeatureFlags"][key];
					}

					purchasedProducts = data["Products"];

					SetDeprecatedFlags();

					// product flags
					SetFlag(flags.ENABLE_CDL, GetProduct(products.CG));

					// update user-level flags
					if (ApiHost !== "/ulysses") {
						UpdateFlagsBasedOnRights();
					}
					actions.setFeatureFlags(flagValues);
					promise.resolve(flagValues);
				})
				.error(function() {
					// if cannot get feature flags from backend
					SetFlag(flags.ALLOW_PIVOT_FILE, false);
					SetFlag(flags.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES, false);
					SetFlag(flags.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE, false);

					SetDeprecatedFlags();

					// LP2
					SetFlag(flags.ADMIN_ALERTS_TAB, false);
					SetFlag(flags.SETUP_PAGE, false);
					SetFlag(flags.ACTIVATE_MODEL_PAGE, false);
					SetFlag(flags.SYSTEM_SETUP_PAGE, false);
					SetFlag(flags.DEPLOYMENT_WIZARD_PAGE, false);
					SetFlag(flags.LEAD_ENRICHMENT_PAGE, false);

					// product flags
					SetFlag(flags.ENABLE_CDL, false);
					SetFlag(flags.ENABLE_ENTITY_MATCH, false);
					SetFlag(flags.ALWAYS_ON_CAMPAIGNS, false);

					// update user-level flags
					if (ApiHost !== "/ulysses") {
						UpdateFlagsBasedOnRights();
					}
					actions.setFeatureFlags(flagValues);
					promise.resolve(flagValues);
				});
		}

		function SetDeprecatedFlags() {
			SetFlag(flags.CAMPAIGNS_PAGE, false);
			SetFlag(flags.USE_SALESFORCE_SETTINGS, true);
			SetFlag(flags.USE_MARKETO_SETTINGS, true);
			SetFlag(flags.USE_ELOQUA_SETTINGS, true);
			SetFlag(flags.LATTICE_MARKETO_PAGE, true);
			SetFlag(flags.ENABLE_FUZZY_MATCH, true);
			SetFlag(flags.ENABLE_DATA_PROFILING_V2, true);
		}

		function SetFlag(flag, value) {
			flagValues[flag] = value;
		}
		function UpdateFlag(flag, value) {
			SetFlagUsingBooleanAnd(flag, value);
		}
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
		function GetFlag(flag) {
			return flagValues[flag] || false;
		}

		/**
		 * @return {boolean}
		 */
		function GetProduct(product) {
			return purchasedProducts.includes(product) || false;
		}

		function UpdateFlagsBasedOnRights() {
			UpdateFlag(
				flags.VIEW_SAMPLE_LEADS,
				RightsUtility.currentUserMay("View", "Sample_Leads")
			);
			UpdateFlag(
				flags.VIEW_REFINE_CLONE,
				RightsUtility.currentUserMay("View", "Refine_Clone")
			);
			UpdateFlag(
				flags.EDIT_REFINE_CLONE,
				RightsUtility.currentUserMay("Edit", "Refine_Clone")
			);

			UpdateFlag(
				flags.VIEW_REMODEL,
				RightsUtility.currentUserMay("View", "Remodel")
			);

			UpdateFlag(
				flags.CHANGE_MODEL_NAME,
				RightsUtility.currentUserMay("Edit", "Models")
			);
			UpdateFlag(
				flags.DELETE_MODEL,
				RightsUtility.currentUserMay("Edit", "Models")
			);
			UpdateFlag(flags.REVIEW_MODEL, false);
			UpdateFlag(
				flags.UPLOAD_JSON,
				RightsUtility.currentUserMay("Create", "Models")
			);

			UpdateFlag(
				flags.USER_MGMT_PAGE,
				RightsUtility.currentUserMay("View", "Users")
			);
			UpdateFlag(
				flags.ADD_USER,
				RightsUtility.currentUserMay("Edit", "Users")
			);
			UpdateFlag(
				flags.CHANGE_USER_ACCESS,
				RightsUtility.currentUserMay("Edit", "Users")
			);
			UpdateFlag(
				flags.DELETE_USER,
				RightsUtility.currentUserMay("Edit", "Users")
			);

			UpdateFlag(
				flags.ADMIN_PAGE,
				RightsUtility.currentUserMay("View", "Reporting")
			);
			UpdateFlag(
				flags.ADMIN_ALERTS_TAB,
				RightsUtility.currentUserMay("Edit", "Configurations")
			);
			UpdateFlag(
				flags.MODEL_HISTORY_PAGE,
				RightsUtility.currentUserMay("View", "Reporting")
			);
			UpdateFlag(
				flags.SYSTEM_SETUP_PAGE,
				RightsUtility.currentUserMay("Edit", "Configurations")
			);
			UpdateFlag(
				flags.ACTIVATE_MODEL_PAGE,
				RightsUtility.currentUserMay("Edit", "Configurations")
			);

			UpdateFlag(
				flags.SETUP_PAGE,
				RightsUtility.currentUserMay("Edit", "Configurations")
			);
			UpdateFlag(
				flags.DEPLOYMENT_WIZARD_PAGE,
				RightsUtility.currentUserMay("Edit", "Configurations")
			);
			UpdateFlag(
				flags.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE,
				RightsUtility.currentUserMay("Edit", "Configurations")
			);
			UpdateFlag(
				flags.LEAD_ENRICHMENT_PAGE,
				RightsUtility.currentUserMay("Edit", "Configurations")
			);

			UpdateFlag(
				flags.JOBS_PAGE,
				RightsUtility.currentUserMay("View", "Jobs")
			);
			UpdateFlag(
				flags.MARKETO_SETTINGS_PAGE,
				RightsUtility.currentUserMay("Edit", "Configurations")
			);
			UpdateFlag(
				flags.API_CONSOLE_PAGE,
				RightsUtility.currentUserMay("Create", "Oauth2Token")
			);
		}
	});
