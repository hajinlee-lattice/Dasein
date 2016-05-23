angular.module('mainApp.create.controller.CustomFieldsController', [
    'mainApp.create.csvImport',
    'mainApp.setup.modals.SelectFieldsModal',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility'
])
.controller('CustomFieldsController', function($scope, $rootScope, $stateParams, ResourceUtility, NavUtility, csvImportService, SelectFieldsModal) {
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

    $scope.$on(NavUtility.MAP_LATTICE_SCHEMA_FIELD_EVENT, function(event, data) {
        mapUserFieldToLatticeField(data.userFieldName, data.latticeSchemaField);
        console.log("mapped: ", data.latticeSchemaField);
    });

    function mapUserFieldToLatticeField(userFieldName, latticeSchemaField) {
        for (var i = 0; i < $scope.fieldMappings.length; i++) {
            if ($scope.fieldMappings[i].userField == userFieldName) {
                $scope.fieldMappings[i].mappedField = latticeSchemaField.name;
                $scope.fieldMappings[i].fieldType = latticeSchemaField.fieldType;
            }
        }
    }

    function showLatticeFieldsSelector (fieldSelected) {
        SelectFieldsModal.show($scope.schema, fieldSelected);
    };
});