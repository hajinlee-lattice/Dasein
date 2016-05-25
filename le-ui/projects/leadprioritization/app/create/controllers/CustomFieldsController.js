angular.module('mainApp.create.controller.CustomFieldsController', [
    'mainApp.create.csvImport',
    'mainApp.setup.modals.SelectFieldsModal',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility'
])
.controller('CustomFieldsController', function($scope, $rootScope, $state, $stateParams, ResourceUtility, NavUtility, csvImportService, csvImportStore, SelectFieldsModal) {
    $scope.csvFileName = $stateParams.csvFileName;
    $scope.schema;
    $scope.fieldMappings = [];
    $scope.fieldNameToFieldMappings = {};
    $scope.fieldNameToFieldTypes = {};
    $scope.mappingOptions = [{ name: "Map to a Lattice Field", id: 0 },
            { name: "Custom Field with the same name", id: 1 },
            { name: "Ignore this field", id: 2}];
    $scope.ignoredFields = [];

    csvImportService.GetFieldDocument($scope.csvFileName).then(function(result) {
        $scope.schema = result.Result.schemaInterpretation;
        $scope.fieldMappings = result.Result.fieldMappings;

        for (var i = 0; i < $scope.fieldMappings.length; i++) {
            var fieldMapping = $scope.fieldMappings[i];
            if (fieldMapping.mappedField == null) {
                $scope.fieldNameToFieldMappings[fieldMapping.userField] = null;
            } else {
                $scope.fieldNameToFieldMappings[fieldMapping.userField] = fieldMapping;
            }
            $scope.fieldNameToFieldTypes[fieldMapping.userField] = fieldMapping.fieldType;
        }
    });

    $scope.mappingChanged = function(fieldMapping, selectedOption) {
        if (selectedOption == null) { // user mapped a field and unmapped it for some reason
            $scope.fieldNameToFieldMappings[fieldMapping.userField] = null;
        } else if (selectedOption == $scope.mappingOptions[0]) {
            showLatticeFieldsSelector(fieldMapping);
        } else if (selectedOption == $scope.mappingOptions[1]) {
            var newCustomFieldMapping = {};
            newCustomFieldMapping.userField = fieldMapping.userField;
            newCustomFieldMapping.mappedField = fieldMapping.userField;
            newCustomFieldMapping.fieldType = $scope.fieldNameToFieldTypes[fieldMapping.userField];
            newCustomFieldMapping.mappedToLatticeField = false;

            $scope.fieldNameToFieldMappings[fieldMapping.userField] = newCustomFieldMapping;
        } else if (selectedOption == $scope.mappingOptions[2]) {
            $scope.fieldNameToFieldMappings[fieldMapping.userField] = {}; // if field is ignored, we'll use {} to designate it.
        }
    };

    $scope.$on(NavUtility.MAP_LATTICE_SCHEMA_FIELD_EVENT, function(event, data) {
        mapUserFieldToLatticeField(data.userFieldName, data.latticeSchemaField);
        console.log("mapped: ", data.latticeSchemaField);
    });

    function deleteFromIgnoredFieldIfExists(fieldName) {
        if (fieldName in $scope.ignoredFields) {
            $scope.ignoredFields.splice($scope.ignoredFields.indexOf(fieldName), 1);
        }
    }

    $scope.csvSubmitColumns = function(event) {
        var fieldMappings = [];
        for (fieldName in $scope.fieldNameToFieldMappings) {
            if ($scope.fieldNameToFieldMappings[fieldName].userField != null) {
                fieldMappings.push($scope.fieldNameToFieldMappings[fieldName]);
            } else {
                $scope.ignoredFields.push(fieldName);
            }
        }

        csvImportService.SaveFieldDocuments($scope.csvFileName, $scope.schema, fieldMappings,
            $scope.ignoredFields).then(function(result) {

            var csvMetadata = csvImportStore.Get($scope.csvFileName);
            csvImportService.StartModeling(csvMetadata).then(function(result) {
                $state.go('home.jobs.status', {'jobCreationSuccess': result.Success });
            });
        });
    };

    $scope.isDocumentCompletelyMapped = function() {
        for (var fieldName in $scope.fieldNameToFieldMappings) {
            if ($scope.fieldNameToFieldMappings[fieldName] == null) {
                return false;
            }
        }
        return true;
    };

    function mapUserFieldToLatticeField(userFieldName, latticeSchemaField) {
        var newUserFieldMapping = { userField: userFieldName };
        newUserFieldMapping.mappedField = latticeSchemaField.name;
        newUserFieldMapping.mappedToLatticeField = true;
        newUserFieldMapping.fieldType = latticeSchemaField.fieldType;

        $scope.fieldNameToFieldMappings[userFieldName] = newUserFieldMapping;
        $scope.fieldNameToFieldTypes[userFieldName] = latticeSchemaField.fieldType;
    }

    function showLatticeFieldsSelector (fieldSelected) {
        SelectFieldsModal.show($scope.schema, fieldSelected);
    };
});