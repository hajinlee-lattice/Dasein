angular.module('mainApp.core.utilities.NavUtility', [])
.service('NavUtility', function () {
    
    // Hash Constants
    this.MANAGE_CREDENTIALS_HASH = "/ManageCredentials";
    this.UPDATE_PASSWORD_HASH = "/UpdatePassword";
    this.USER_MANAGEMENT_HASH = "/UserManagement";
    this.MODEL_LIST_HASH = "/ModelList";
    this.MODEL_DETAIL_HASH = "/ModelDetail";
    this.MODEL_CREATION_HISTORY_HASH = "/ModelCreationHistory";
    this.ACTIVATE_MODEL = "/ActivateModel";
    
    // Navigation Event Constants
    this.MANAGE_CREDENTIALS_NAV_EVENT = "ManageCredentialsNavEvent";
    this.UPDATE_PASSWORD_NAV_EVENT = "UpdatePasswordNavEvent";
    this.USER_MANAGEMENT_NAV_EVENT = "UserManagementNavEvent";
    this.MODEL_LIST_NAV_EVENT = "ModelListNavEvent";
    this.MODEL_DETAIL_NAV_EVENT = "ModelDetailNavEvent";
    this.MODEL_CREATION_HISTORY_NAV_EVENT = "ModelCreationHistoryNavEvent";
    
    // General Event Constants
    this.SYSTEM_CONFIGURED_COMPLETE_EVENT = "SystemConfiguredCompleteEvent";
});