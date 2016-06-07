angular.module('mainApp.create.controller.CustomFieldsController', [
    'mainApp.create.csvImport',
    'mainApp.setup.modals.SelectFieldsModal',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility'
])
.controller('CustomFieldsController', function($state, $stateParams, ResourceUtility, csvImportService, csvImportStore, FieldDocument, UnmappedFields) {
    var vm = this;

    angular.extend(vm, {
        FormValidated: true,
        ResourceUtility : ResourceUtility,
        csvFileName: $stateParams.csvFileName,
        mappingOptions: [
            { id: 0, name: "Custom Field Name" },
            { id: 1, name: "Map to Lattice Data Cloud" },
            { id: 2, name: "Ignore this field" }
        ],
        ignoredFields: FieldDocument.ignoredFields = [],
        UnmappedFields: UnmappedFields[FieldDocument.schemaInterpretation],
        fieldMappings: FieldDocument.fieldMappings,
        schema: FieldDocument.schemaInterpretation,
        RequiredFields: []
    });

    vm.init = function() {
        vm.refreshLatticeFields();
        vm.UnmappedFields.forEach(function(field, index) {
            if (field.requiredType == 'Required') {
                vm.RequiredFields.push(field.name);
            }
        });
        console.log('init', this);
    }

    vm.changeMappingOption = function(fieldMapping, selectedOption) {
        fieldMapping.mappedToLatticeField = false;
        delete fieldMapping.ignored;

        switch (selectedOption.id) {
            case 0: 
                fieldMapping.mappedField = fieldMapping.mappedField || fieldMapping.userField;
                break;
            case 1: 
                fieldMapping.mappedField = vm.UnmappedFieldsMap[fieldMapping.mappedField] 
                    ? fieldMapping.mappedField 
                    : '';

                fieldMapping.mappedToLatticeField = true;
                break;
            case 2: 
                fieldMapping.ignored = true; 
                break;
        }

        vm.refreshLatticeFields();
        vm.validateForm();
    }

    vm.changeLatticeField = function(fieldMapping) {
        fieldMapping.mappedToLatticeField = true;
        fieldMapping.fieldType = vm.UnmappedFieldsMap[fieldMapping.mappedField].fieldType;
        
        vm.refreshLatticeFields();
        vm.validateForm();
    }

    vm.refreshLatticeFields = function() {
        vm.fieldMappingsMapped = {};
        vm.UnmappedFieldsMap = {};
        vm.fieldMappings.forEach(function(fieldMapping, index) {
            if (fieldMapping.mappedField && !fieldMapping.ignored) {
                vm.fieldMappingsMapped[fieldMapping.mappedField] = fieldMapping;
            }
        });
        vm.UnmappedFields.forEach(function(UnmappedField, index) {
            vm.UnmappedFieldsMap[UnmappedField.name] = UnmappedField;
        });
    }

    vm.clickNext = function() {
        ShowSpinner('Saving Field Mappings...');

        vm.fieldMappings.forEach(function(fieldMapping, index) {
            if (fieldMapping.ignored) {
                vm.ignoredFields.push(fieldMapping.userField);

                delete fieldMapping.ignored;
            }
        });

        csvImportService.SaveFieldDocuments(vm.csvFileName, FieldDocument).then(function(result) {
            ShowSpinner('Executing Modeling Job...');

            var csvMetadata = csvImportStore.Get(vm.csvFileName);
            
            csvImportService.StartModeling(csvMetadata).then(function(result) {
                $state.go('home.jobs.status', { 'jobCreationSuccess': result.Success });
            });
        });
    }

    vm.validateForm = function() {
        vm.FormValidated = true;

        vm.fieldMappings.forEach(function(fieldMapping, index) {
            if (!fieldMapping.mappedField && fieldMapping.mappedToLatticeField) {
                vm.FormValidated = false;
            }
        });

        vm.RequiredFields.forEach(function(requiredField, index) {
            if (!vm.fieldMappingsMapped[requiredField]) {
                vm.FormValidated = false;
            }
        });
    }

    vm.init();
});
/*
.controller('CustomFieldsController-bak', function($scope, $rootScope, $state, $stateParams, ResourceUtility, NavUtility, csvImportService, csvImportStore, SelectFieldsModal, FieldDocument) {
    $scope.csvFileName = $stateParams.csvFileName;
    $scope.schema;
    $scope.fieldMappings = [];
    $scope.fieldNameToFieldMappings = {};
    $scope.fieldNameToFieldTypes = {};
    $scope.mappingOptions = [
        { name: "Custom Field Name", id: 0 },
        { name: "Map to Lattice Data Cloud", id: 1 },
        { name: "Ignore this field", id: 2 }
    ];
    $scope.ignoredFields = [];

    $scope.schema = FieldDocument.schemaInterpretation;
    $scope.fieldMappings = FieldDocument.fieldMappings;    $scope.mappingOptions = [
        { name: "Custom Field Name", id: 0 },
        { name: "Map to Lattice Data Cloud", id: 1 },
        { name: "Ignore this field", id: 2 }
    ];
    $scope.ignoredFields = [];

    $scope.schema = FieldDocument.schemaInterpretation;
    $scope.fieldMappings = FieldDocument.fieldMappings;

    $scope.mappingChanged = function(fieldMapping, selectedOption) {
        if (selectedOption == $scope.mappingOptions[1]) {
            showLatticeFieldsSelector(fieldMapping);
        } else if (selectedOption == $scope.mappingOptions[0]) {
            fieldMapping.userField = fieldMapping.userField;
            fieldMapping.mappedField = fieldMapping.userField;
            //fieldMapping.fieldType = $scope.fieldNameToFieldTypes[fieldMapping.userField];
            fieldMapping.mappedToLatticeField = false;

            if (!fieldMapping.mappedField) {
                fieldMapping.mappedField = fieldMapping.userField;
            }
        } else if (selectedOption == $scope.mappingOptions[2]) {
            $scope.fieldNameToFieldMappings[fieldMapping.userField] = {}; // if field is ignored, we'll use {} to designate it.
        }
    };

    for (var i = 0; i < $scope.fieldMappings.length; i++) {
        var fieldMapping = $scope.fieldMappings[i];
        if (fieldMapping.mappedField == null) {
            $scope.mappingChanged(fieldMapping, $scope.mappingOptions[0])
        } else {
        }
        $scope.fieldNameToFieldMappings[fieldMapping.userField] = fieldMapping;
        $scope.fieldNameToFieldTypes[fieldMapping.userField] = fieldMapping.fieldType;
    }

    $scope.$on(NavUtility.MAP_LATTICE_SCHEMA_FIELD_EVENT, function(event, data) {
        mapUserFieldToLatticeField(data.userFieldName, data.latticeSchemaField);
    });

    $scope.$on('Mapped_Field_Modal_Cancelled', function(event, data) {
        console.log(event, data);
        mapUserFieldToLatticeField(data.userFieldName, data.latticeSchemaField);
        $scope.mappingChanged(data.latticeSchemaField, $scope.mappingOptions[0]);
    });

    function deleteFromIgnoredFieldIfExists(fieldName) {
        if (fieldName in $scope.ignoredFields) {
            $scope.ignoredFields.splice($scope.ignoredFields.indexOf(fieldName), 1);
        }
    }

    $scope.csvSubmitColumns = function() {
        var fieldMappings = [];
        for (fieldName in $scope.fieldNameToFieldMappings) {
            if ($scope.fieldNameToFieldMappings[fieldName].userField != null) {
                fieldMappings.push($scope.fieldNameToFieldMappings[fieldName]);
            } else {
                $scope.ignoredFields.push(fieldName);
            }
        }

        ShowSpinner('Saving Field Mappings...')

        csvImportService.SaveFieldDocuments(
            $scope.csvFileName, 
            $scope.schema, 
            fieldMappings,
            $scope.ignoredFields
        ).then(function(result) {
            var csvMetadata = csvImportStore.Get($scope.csvFileName);
            
            ShowSpinner('Executing Modeling Job...');

            csvImportService.StartModeling(csvMetadata).then(function(result) {
                $state.go('home.jobs.status', {'jobCreationSuccess': result.Success });

                if (result.Result && result.Result != "") {
                    setTimeout(function() {
                        $state.go('home.models.import.job', { applicationId: result.Result });
                    }, 1);
                } else {
                    // ERROR
                }
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
        csvImportStore.CurrentFieldMapping = fieldSelected;
        csvImportStore.fieldNameToFieldMappings = $scope.fieldNameToFieldMappings;
        csvImportStore.fieldMappings = $scope.fieldMappings;

        SelectFieldsModal.show($scope.schema, fieldSelected);
    };
});
*/