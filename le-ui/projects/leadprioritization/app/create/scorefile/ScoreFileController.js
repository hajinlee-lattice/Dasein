angular.module('mainApp.create.csvBulkUpload', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.leadenrichment',
    'mainApp.core.utilities.NavUtility'
])
.controller('csvBulkUploadController', function(
    $state, $stateParams, ResourceUtility, ImportService, ImportStore, StringUtility,
    ScoreLeadEnrichmentModal, Model, IsPmml
) {
    var vm = this;

    vm.importErrorMsg = "";
    vm.importing = false;
    vm.uploaded = false;
    vm.showImportError = null;
    vm.showImportSuccess = false;
    vm.accountLeadCheck = false;
    vm.ResourceUtility = ResourceUtility;
    vm.schema = Model.ModelDetails.SourceSchemaInterpretation == 'SalesforceLead' ? 'Lead' : 'Account';
    vm.isPmml = IsPmml;


    console.log(vm.showImportError);

    vm.params = {
        url: '/pls/scores/fileuploads',
        label: vm.schema + ' List',
        infoTemplate: (vm.schema == 'Lead' ? 'Upload a CSV file with leads to score. The list of expected column headers is displayed below.' : 'Upload a CSV file with accounts to score. The list of expected column headers is displayed below.'),
        defaultMessage: "Example: us-target-list.csv",
        modelId: $stateParams.modelId,
        compressed: true,
        schema: null,
        noSizeLimit: true
    }

    vm.fileSelect = function(result) {
        vm.uploaded = false;
    }

    vm.fileLoad = function(result) {

    }

    vm.fileDone = function(result) {
        if (result.Result && result.Result.name) {
            vm.uploaded = true;
            vm.Result = result.Result;
            vm.fileName = result.Result.name;
        }
    }

    vm.fileCancel = function() {
        var xhr = ImportStore.Get('cancelXHR', true);

        if (xhr) {
            xhr.abort();
        }
    }

    vm.clickNext = function() {
        if (IsPmml) {
            ShowSpinner('Executing Scoring Job...');

            ImportService.StartTestingSet(vm.params.modelId, vm.fileName, false).then(function(result) {
                $state.go('home.model.jobs', { 'jobCreationSuccess': (!!vm.fileName) });
            });
        } else {
            var fileName = fileName || vm.fileName,
                modelName = StringUtility.SubstituteAllSpecialCharsWithDashes(vm.Result.display_name),
                metaData = vm.metadata = vm.metadata || {};

            metaData.name = fileName;
            metaData.modelName = modelName;
            metaData.displayName = vm.Result.display_name;
            metaData.description = vm.Result.description;
            metaData.schemaInterpretation = vm.Result.schema_interpretation;

            ImportStore.Set(fileName, metaData);

            setTimeout(function() {
                $state.go('home.model.scoring.mapping', { csvFileName: fileName });
            }, 1);
        }
    
    }
});
