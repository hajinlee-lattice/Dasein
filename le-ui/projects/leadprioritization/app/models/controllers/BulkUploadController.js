angular.module('mainApp.create.csvBulkUpload', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.NavUtility'
])
.controller('csvBulkUploadController', function($state, $stateParams, ResourceUtility, ImportService, ImportStore, RequiredFields, Model) {
    var vm = this;

    vm.importErrorMsg = "";
    vm.importing = false;
    vm.showImportError = false;
    vm.showImportSuccess = false;
    vm.accountLeadCheck = false;
    vm.ResourceUtility = ResourceUtility;
    vm.requiredFields = RequiredFields;
    vm.params = {
        url: '/pls/scores/fileuploads',
        defaultMessage: "Example: us-enterprise-testing-set.csv",
        modelId: $stateParams.modelId,
        compressed: true, 
        schema: null
    }

    vm.fileDone = function(result) {
        if (result.Result && result.Result.name) {
            vm.fileName = result.Result.name;
        }
    }

    vm.fileCancel = function() {
        ImportStore.Get('cancelXHR', true).abort();

        $state.go('home.model.jobs');
    }

    vm.clickNext = function() {
        ShowSpinner('Executing Scoring Job...');

        ImportService.StartTestingSet(vm.params.modelId, vm.fileName).then(function(result) {
            $state.go('home.model.jobs', { 'jobCreationSuccess': (!!vm.fileName) });
        });
    }
});