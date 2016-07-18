angular.module('mainApp.create.csvBulkUpload', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.leadenrichment',
    'mainApp.core.utilities.NavUtility'
])
.controller('csvBulkUploadController', function($state, $stateParams, ResourceUtility, ImportService, ImportStore, ScoreLeadEnrichmentModal, RequiredFields, Model) {
    var vm = this;

    vm.importErrorMsg = "";
    vm.importing = false;
    vm.uploaded = false;
    vm.showImportError = false;
    vm.showImportSuccess = false;
    vm.accountLeadCheck = false;
    vm.ResourceUtility = ResourceUtility;
    vm.requiredFields = RequiredFields;
    vm.schema = Model.ModelDetails.SourceSchemaInterpretation;

    vm.params = {
        url: '/pls/scores/fileuploads',
        label: (vm.schema == 'SalesforceLead' ? 'Lead' : 'Account') + ' List',
        infoTemplate: (vm.schema == 'SalesforceLead' ? 'Upload a CSV file with leads to score. The list of expected column headers is displayed below.' : 'Upload a CSV file with accounts to score. The list of expected column headers is displayed below.'),
        defaultMessage: "Example: us-enterprise-testing-set.csv",
        modelId: $stateParams.modelId,
        compressed: true, 
        schema: null
    }

    vm.fileSelect = function(result) {
        vm.uploaded = false;
    }

    vm.fileLoad = function(result) {
    
    }

    vm.fileDone = function(result) {
        if (result.Result && result.Result.name) {
            vm.uploaded = true;
            vm.fileName = result.Result.name;
        }
    }

    vm.fileCancel = function() {
        ImportStore.Get('cancelXHR', true).abort();
    }

    vm.clickNext = function() {
        ScoreLeadEnrichmentModal.showFileScoreModal(vm.params.modelId, vm.fileName);
    }
});
