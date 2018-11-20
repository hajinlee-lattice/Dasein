angular.module('mainApp.setup.utilities.SetupUtility', [])
.service('SetupUtility', function () {

    // Event Constants
    this.LOAD_FIELDS_EVENT = "LoadFieldsEvent";

    // Tenant Deployment Constants
    this.STEP_ENTER_CREDENTIALS = "ENTER_CREDENTIALS";
    this.STEP_IMPORT_DATA = "IMPORT_SFDC_DATA";
    this.STEP_ENRICH_DATA = "ENRICH_DATA";
    this.STEP_VALIDATE_METADATA = "VALIDATE_METADATA";
    this.STATUS_IN_PROGRESS = "IN_PROGRESS";
    this.STATUS_SUCCESS = "SUCCESS";
    this.STATUS_FAIL = "FAIL";
    this.STATUS_WARNING = "WARNING";
});