export const VIEW_SAMPLE_LEADS = "View_Sample_Leads";
export const VIEW_REFINE_CLONE = "View_Refine_Clone";
export const EDIT_REFINE_CLONE = "Edit_Refine_Clone";
export const VIEW_REMODEL = "View_Remodel";
export const CHANGE_MODEL_NAME = "ChangeModelNames";
export const DELETE_MODEL = "DeleteModels";
export const REVIEW_MODEL = "ReviewModel";
export const UPLOAD_JSON = "UploadSummaryJson";
export const USER_MGMT_PAGE = "UserManagementPage";
export const ADD_USER = "AddUsers";
export const CHANGE_USER_ACCESS = "ChangeUserAccess";
export const DELETE_USER = "DeleteUsers";
export const ADMIN_PAGE = "AdminPage";
export const MODEL_HISTORY_PAGE = "ModelCreationHistoryPage";
export const JOBS_PAGE = "JobsPage";
export const MARKETO_SETTINGS_PAGE = "MarketoSettingsPage";
export const API_CONSOLE_PAGE = "APIConsolePage";
// ===================================
// END= flags governed by user level
// ===================================

// ====================
// BEGIN= product flags
// ====================
// These are actually product flag (whether the customer has purchased the product or not)
export const ENABLE_CDL = "EnableCdl";
export const ENABLE_ENTITY_MATCH = "EnableEntityMatch";
export const ALWAYS_ON_CAMPAIGNS = "AlwaysOnCampaigns";
// ====================
// END= product flags
// ====================

// ================
// BEGIN= LP2 flags
// ================
export const ADMIN_ALERTS_TAB = "AdminAlertsTab";
export const SETUP_PAGE = "SetupPage";
export const DEPLOYMENT_WIZARD_PAGE = "DeploymentWizardPage";
export const SYSTEM_SETUP_PAGE = "SystemSetupPage";
export const ACTIVATE_MODEL_PAGE = "ActivateModelPage";
export const LEAD_ENRICHMENT_PAGE = "LeadEnrichmentPage";
// ================
// END= LP2 flags
// ================

export const REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE =
  "RedirectToDeploymentWizardPage";
export const ALLOW_PIVOT_FILE = "AllowPivotFile";
export const ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES =
  "EnableInternalEnrichmentAttributes";
export const LATTICE_INSIGHTS = "LatticeInsights";
export const VDB_MIGRATION = "VDBMigration";
export const SCORE_EXTERNAL_FILE = "ScoreExternalFile";
export const ENABLE_CROSS_SELL_MODELING = "EnableCrossSellModeling";
export const ENABLE_FILE_IMPORT = "EnableFileImport";
export const ENABLE_PRODUCT_BUNDLE_IMPORT = "EnableProductBundleImport";
export const ENABLE_PRODUCT_HIERARCHY_IMPORT = "EnableProductHierarchyImport";
export const ENABLE_PRODUCT_PURCHASE_IMPORT = "EnableProductPurchaseImport";
export const PLAYBOOK_MODULE = "PlaybookModule";
export const LAUNCH_PLAY_TO_MAP_SYSTEM = "LaunchPlayToMapSystem";
export const ENABLE_EXTERNAL_INTEGRATION = "EnableExternalIntegration";
export const MIGRATION_TENANT = "MigrationTenant";
export const ENABLE_MULTI_TEMPLATE_IMPORT = "EnableMultiTemplateImport";
export const ALPHA_FEATURE = "AlphaFeature";

import { reducer } from "./featureFlags.redux";
import { store, injectAsyncReducer } from "../../app/store/index";
import { isEmpty } from "../utilities/ObjectUtilities.js";

class FeatureFlagsUtilities {
  constructor() {
    if (!FeatureFlagsUtilities.instance) {
      FeatureFlagsUtilities.instance = this;
      injectAsyncReducer(store, "featureflags", reducer);
    }
    return FeatureFlagsUtilities.instance;
  }

  // init() {}
  isFeatureFlagEnabled(featureFlagName) {
    const state = store.getState()["featureflags"];
    let ret = state.featureFlags[featureFlagName] || false;
    return ret;
  }
}

const instance = new FeatureFlagsUtilities();
Object.freeze(instance);

export default instance;
