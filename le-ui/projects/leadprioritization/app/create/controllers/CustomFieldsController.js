angular
.module('mainApp.create.csvImport')
.controller('CustomFieldsController', function($scope, $state, $stateParams, ResourceUtility, csvImportService, csvImportStore, FieldDocument, UnmappedFields) {
    var vm = this;

    angular.extend(vm, {
        FormValidated: true,
        ResourceUtility : ResourceUtility,
        csvFileName: $stateParams.csvFileName,
        mappingOptions: [
            { id: 0, name: "Use as Custom Predictor" },
            { id: 1, name: "Map to Standard Field" },
            { id: 2, name: "Ignore this field" }
        ],
        ignoredFields: FieldDocument.ignoredFields = [],
        fieldMappings: FieldDocument.fieldMappings,
        RequiredFields: []
    });

    vm.init = function() {
        vm.csvMetadata = csvImportStore.Get($stateParams.csvFileName) || {};
        vm.schema = vm.csvMetadata.schemaInterpretation || 'SalesforceLead';
        vm.UnmappedFields = UnmappedFields[vm.schema] || [];

        vm.UnmappedFields.forEach(function(field, index) {
            if (field.requiredType == 'Required') {
                vm.RequiredFields.push(field.name);
            }
        });

        vm.refreshLatticeFields();
    }

    vm.changeMappingOption = function(mapping, selectedOption) {
        mapping.mappedToLatticeField = false;
        delete mapping.ignored;

        switch (selectedOption.id) {
            case 0: // custom user mapping
                mapping.mappedField = mapping.mappedField || mapping.userField;
                break;
            case 1: // map to lattice cloud
                mapping.mappedField = vm.UnmappedFieldsMap[mapping.mappedField] 
                    ? mapping.mappedField 
                    : '';

                mapping.mappedToLatticeField = true;
                break;
            case 2: // ignore this field
                mapping.ignored = true; 
                break;
        }

        vm.refreshLatticeFields();

        setTimeout(function() {
            vm.validateForm();
            $scope.$digest();
        },1);
    }

    vm.changeLatticeField = function(mapping) {
        mapping.mappedToLatticeField = true;
        mapping.fieldType = vm.UnmappedFieldsMap[mapping.mappedField].fieldType;
        
        vm.refreshLatticeFields();

        setTimeout(function() {
            vm.validateForm();
            $scope.$digest();
        },1);
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

        // build ignoredFields list from temp 'ignored' fieldMapping property
        vm.fieldMappings.forEach(function(fieldMapping, index) {
            if (fieldMapping.ignored) {
                vm.ignoredFields.push(fieldMapping.userField);

                delete fieldMapping.ignored;
            }
        });

        csvImportService.SaveFieldDocuments(vm.csvFileName, FieldDocument).then(function(result) {
            ShowSpinner('Executing Modeling Job...');

            csvImportService.StartModeling(vm.csvMetadata).then(function(result) {
                if (result.Result && result.Result != "") {
                    setTimeout(function() {
                        $state.go('home.models.import.job', { applicationId: result.Result });
                    }, 1);
                }
            });
        });
    }

    vm.validateMappingSelect = function(mapping) {
        var name = 'mapping_lattice_field_select_';

        vm.validateIsDuplicate(name, mapping);
    }

    vm.validateMappingInput = function(mapping) {
        var name = 'mapping_custom_field_input_';

        vm.validateIsReserved(name, mapping);
        vm.validateIsDuplicate(name, mapping);
    }

    vm.validateIsReserved = function(name, mapping) {
        var isReserved = !!vm.UnmappedFieldsMap[mapping.mappedField];
        
        if ($scope.fieldMappingForm[name + mapping.userField]) {
            $scope.fieldMappingForm[name + mapping.userField].$setValidity("Reserved", !isReserved);
        }
    }

    vm.validateIsDuplicate = function(name, mapping) {
        var value = mapping.mappedField;
        var isDuplicate = false;

        vm.fieldMappings.forEach(function(field) {
            if (field.mappedField == value && !field.ignored) {
                if (mapping.userField != field.userField) {
                    isDuplicate = true;
                }
            }
        }); 
        
        if ($scope.fieldMappingForm[name + mapping.userField]) {
            $scope.fieldMappingForm[name + mapping.userField].$setValidity("Duplicate", !isDuplicate);
        }
    }

    // here are additional checks not covered by angular's built in form validation
    vm.validateForm = function() {
        vm.FormValidated = true;

        // make sure there are no empty drop-down selection
        vm.fieldMappings.forEach(function(fieldMapping, index) {
            if (fieldMapping.ignored) {
                return;
            }

            if (!fieldMapping.mappedField && fieldMapping.mappedToLatticeField) {
                vm.FormValidated = false;
            }

            if (fieldMapping.mappedToLatticeField) {
                vm.validateMappingSelect(fieldMapping);
            } else {
                vm.validateMappingInput(fieldMapping);
            }
        });

        // make sure all lattice required fields are mapped
        vm.RequiredFields.forEach(function(requiredField, index) {
            if (!vm.fieldMappingsMapped[requiredField]) {
                vm.FormValidated = false;
            }
        });
    }

    vm.init();
});
