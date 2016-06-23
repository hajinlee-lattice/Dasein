angular.module('mainApp.create.csvBulkUpload', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.utilities.NavUtility'
])
.controller('csvBulkUploadController', function($rootScope, $stateParams, ModelService, ResourceUtility, $state, $q, csvImportService, csvImportStore, RequiredFields) {
    var vm = this;

    vm.showImportError = false;
    vm.importErrorMsg = "";
    vm.importing = false;
    vm.showImportSuccess = false;
    vm.ResourceUtility = ResourceUtility;
    vm.accountLeadCheck = false;
    vm.requiredFields = RequiredFields;
    vm.params = {
        url: '/pls/scores/fileuploads',
        modelId: $stateParams.modelId,
        compressed: true, 
        schema: null
    }

    vm.uploadFile = function() { }

    vm.processHeaders = function(headers) { }

    vm.generateModelName = function(fileName) { }
    
    vm.uploadResponse = function(result) {
        if (result.Result && result.Result.name) {
            vm.fileName = result.Result.name;
        }
    }

    vm.cancelClicked = function() {
        csvImportStore.Get('cancelXHR', true).abort();
        $state.go('home.model.jobs');
    }

    vm.clickNext = function() {
        ShowSpinner('Executing Scoring Job...');
        csvImportService.StartTestingSet(vm.params.modelId, vm.fileName).then(function(result) {
            $state.go('home.model.jobs', { 'jobCreationSuccess': (!!vm.fileName) });
        });
    }
});