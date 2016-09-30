angular
.module('lp.create.import')
.controller('ScoreFieldsController', function(
    $scope, $state, $stateParams, $timeout, ResourceUtility, ScoreLeadEnrichmentModal,
    ImportService, ImportStore, FieldDocument, UnmappedFields, CancelJobModal
) {
    var vm = this;

    angular.extend(vm, {
        FormValidated: true,
        ResourceUtility: ResourceUtility,
        csvFileName: $stateParams.csvFileName,
        mappingOptions: [
            { id: 0, name: "Use as Custom Predictor" },
            { id: 1, name: "Map to Standard Field" },
            { id: 2, name: "Ignore this field" }
        ],
        ignoredFields: FieldDocument.ignoredFields,
        fieldMappings: FieldDocument.fieldMappings,
        fieldsMap: {},
        RequiredFields: ['id','email','event'],
        FileHeaders: [],
        UserFields: [],
        MappedFields: [],
        AvailableFields: [],
        initialized: false,
        modelId: $stateParams.modelId
    });

    vm.init = function() {
        vm.initialized = true;
        vm.csvMetadata = ImportStore.Get($stateParams.csvFileName) || {};
        vm.schema = vm.csvMetadata.schemaInterpretation || 'SalesforceLead';
        vm.UnmappedFields = UnmappedFields || [];
        
        vm.refreshLatticeFields();

        vm.UnmappedFields.forEach(function(field, index) {
            if (vm.ignoredFields.indexOf(field) < 0) {
                vm.FileHeaders.push(field);
            }
        });

        console.log('scorefields', vm);
    }

    vm.changeMappingOption = function(mapping, selectedOption) {
        mapping.mappedToLatticeField = mapping.mappedToLatticeField || false;
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
        //mapping.fieldType = vm.UnmappedFieldsMap[mapping.mappedField].fieldType;
        
        vm.refreshLatticeFields();

        setTimeout(function() {
            //vm.validateForm();
            $scope.$digest();
        },1);
    }

    vm.refreshLatticeFields = function() {
        vm.UserFields = [];
        vm.MappedFields = [];
        vm.AvailableFields = [];

        for (var i=0; i < vm.fieldMappings.length; i++) {
            var field = vm.fieldMappings[i];

            if (field.userField) {
                vm.UserFields.push(field.userField);
            }

            if (field.mappedField) {
                vm.MappedFields.push(field.mappedField);
                vm.fieldsMap[field.mappedField] = field;
            }

        }
        
        vm.MappedFields.forEach(function(field, index) {
            if (vm.ignoredFields.indexOf(field) < 0 && vm.UserFields.indexOf(field) < 0 && vm.AvailableFields.indexOf(field) < 0) {
                vm.AvailableFields.push(field);
            }
        });
    }

    vm.resetClicked = function($event) {
        /*
        ImportStore.ResetAdvancedSettings();
        $state.go('home.models.import');
        */
        if ($event != null) {
            $event.stopPropagation();
        }
        CancelJobModal.show(null, {resetImport:true});
    };

    vm.clickNext = function() {
        ShowSpinner('Saving Field Mappings...');

        // build ignoredFields list from temp 'ignored' fieldMapping property
        /*
        vm.fieldMappings.forEach(function(fieldMapping, index) {
            if (fieldMapping.ignored) {
                vm.ignoredFields.push(fieldMapping.userField);

                delete fieldMapping.ignored;
            }
        });
        */

        ImportService.SaveFieldDocuments(vm.csvFileName, FieldDocument, true).then(function(result) {
            ShowSpinner('Executing Modeling Job...');

            ImportService.StartModeling(vm.csvMetadata).then(function(result) {
                if (result.Result && result.Result != "") {
                    setTimeout(function() {
                        ScoreLeadEnrichmentModal.showFileScoreModal(vm.modelId, vm.csvFileName);
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

    //$timeout(function() {
        vm.init();
    //}, 1);
});
