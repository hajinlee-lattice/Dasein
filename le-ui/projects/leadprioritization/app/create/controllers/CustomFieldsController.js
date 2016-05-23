angular.module('mainApp.create.controller.CustomFieldsController', [
    'mainApp.create.csvImport',
    'mainApp.setup.modals.SelectFieldsModal',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('CustomFieldsController', function($scope, $rootScope, $stateParams, ResourceUtility, csvImportService, SelectFieldsModal) {
    $scope.csvFileName = $stateParams.csvFileName;
    $scope.schema;
    $scope.fieldMappings;
    $scope.mappingOptions = [{ name: "Map to a Lattice Field", id: 0 },
            { name: "Custom Field with the same name", id: 1 },
            { name: "Ignore this field", id: 2}];

    csvImportService.GetFieldDocument($scope.csvFileName).then(function(result) {
        $scope.schema = result.Result.schemaInterpretation;
        $scope.fieldMappings = result.Result.fieldMappings;
    });

    $scope.mappingChanged = function(fieldMapping, selectedOption) {
        if (selectedOption.name == $scope.mappingOptions[0].name) {
            showLatticeFieldsSelector(fieldMapping);
        }
    };

    function showLatticeFieldsSelector (fieldSelected) {
        SelectFieldsModal.show($scope.schema, fieldSelected);
    };
});