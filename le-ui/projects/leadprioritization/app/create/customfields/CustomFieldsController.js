angular
.module('lp.create.import')
.controller('CustomFieldsController', function(
    $scope, $state, $stateParams, $timeout, $anchorScroll, ResourceUtility, FeatureFlagService,
    ImportService, ImportStore, FieldDocument, UnmappedFields, CancelJobModal
) {
    var vm = this;

    angular.extend(vm, {
        FormValidated: true,
        ResourceUtility: ResourceUtility,
        csvFileName: $stateParams.csvFileName,
        fuzzyMatchEnabled: FeatureFlagService.FlagIsEnabled(FeatureFlagService.Flags().ENABLE_FUZZY_MATCH),
        mappingOptions: [
            { id: 0, name: "Custom Predictor" },
            { id: 1, name: "Standard Field" },
            { id: 2, name: "Ignore this field" }
        ],
        ignoredFields: FieldDocument.ignoredFields = [],
        fieldMappings: FieldDocument.fieldMappings,
        initialized: false,
        NextClicked: false,
        standardFieldsList: ['Event', 'Id', null, 'CompanyName', 'State', 'PostalCode', 'Country', 'PhoneNumber'],
        requiredFieldsMissing: {
            'Event': true,
            'Id': true
        },
        ignoredFieldLabel: '-- Unmapped Field --',
        UnmappedFieldsMap: {},
        standardFieldMappings: {},
        AvailableFields: []
    });

    vm.init = function() {
        vm.initialized = true;
        vm.csvMetadata = ImportStore.Get($stateParams.csvFileName) || {};
        vm.schema = vm.csvMetadata.schemaInterpretation || 'SalesforceLead';
        vm.UnmappedFields = UnmappedFields[vm.schema] || [];

        vm.standardFieldsList[2] = (vm.schema === 'SalesforceLead') ? 'Email' : 'Website';
        vm.requiredFieldsMissing[vm.standardFieldsList[2]] = true;

        if (vm.fuzzyMatchEnabled) {
            vm.requiredFieldsMissing['CompanyName'] = true;
        }

        vm.UnmappedFields.forEach(function(UnmappedField) {
            vm.UnmappedFieldsMap[UnmappedField.name] = UnmappedField;
        });

        var fieldMappingsMap = {};
        vm.fieldMappings.forEach(function(fieldMapping) {
            fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
        });

        vm.standardFieldsList.forEach(function(field) {
            // create a copy of mapping object to preserve FieldDocument
            // FieldDocument updated when NextClicked
            if (fieldMappingsMap[field]) {
                vm.standardFieldMappings[field] = angular.copy(fieldMappingsMap[field]);
                vm.standardFieldMappings[field].mappedToLatticeField = true;
            } else {
                vm.standardFieldMappings[field] = {
                    fieldType: vm.UnmappedFieldsMap[field] ? vm.UnmappedFieldsMap[field].fieldType : null,
                    mappedField: field,
                    mappedToLatticeField: true,
                    userField: vm.ignoredFieldLabel
                };
            }
        });

        vm.refreshLatticeFields();
    };

    vm.changeMappingOption = function(mapping, selectedOption) {
        vm.setMappingOption(mapping, selectedOption);

        vm.refreshLatticeFields();
    };

    vm.setMappingOption = function(mapping, selectedOption) {
        mapping.mappedToLatticeField = mapping.mappedToLatticeField || false;
        delete mapping.ignored;

        switch (selectedOption.id) {
            case 0: // custom user mapping
                mapping.mappedField = mapping.userField;
                mapping.mappedToLatticeField = false;
                break;
            case 1: // map to lattice cloud
                mapping.mappedField = vm.UnmappedFieldsMap[mapping.mappedField]
                    ? mapping.mappedField
                    : '';

                mapping.mappedToLatticeField = true;
                break;
            case 2: // ignore this field
                mapping.mappedField = mapping.userField;
                mapping.mappedToLatticeField = false;
                mapping.ignored = true;
                break;
        }
    };

    vm.changeLatticeField = function(mapping) {
        mapping.mappedToLatticeField = true;
        mapping.fieldType = vm.UnmappedFieldsMap[mapping.mappedField] ? vm.UnmappedFieldsMap[mapping.mappedField].fieldType : null;

        vm.refreshLatticeFields();
    };

    vm.refreshLatticeFields = function() {
        vm.fieldMappingsMapped = {};

        vm.fieldMappings.forEach(function(fieldMapping) {
            if (fieldMapping.mappedField && !fieldMapping.ignored &&
                fieldMapping.mappedToLatticeField) {

                vm.fieldMappingsMapped[fieldMapping.mappedField] = fieldMapping;
            }
        });

        if (!vm.NextClicked) {
            vm.AvailableFields = [];

            var usedUserField = {};

            for (var standardFieldMap in vm.standardFieldMappings) {
                var fieldMapping = vm.standardFieldMappings[standardFieldMap];

                var userField = fieldMapping.userField;
                usedUserField[userField] = true;
            }

            vm.fieldMappings.forEach(function(fieldMapping) {
                var userField = fieldMapping.userField;
                if (!usedUserField[userField]) {
                    vm.AvailableFields.push(userField);
                }
            });
        }

        $timeout(vm.validateForm, 0);
    };

    vm.resetClicked = function($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        CancelJobModal.show(null, {resetImport:true});
    };

    vm.clickNext = function() {
        vm.NextClicked = true;

        var userFieldMappingsMap = {},
            mappedFieldMappingsMap = {};

        vm.fieldMappings.forEach(function(fieldMapping) {
            userFieldMappingsMap[fieldMapping.userField] = fieldMapping;

            if (fieldMapping.mappedField) {
                mappedFieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
            }
        });

        for (var standardField in vm.standardFieldMappings) {
            var stdFieldMapping = vm.standardFieldMappings[standardField];

            if (stdFieldMapping.userField && stdFieldMapping.userField !== vm.ignoredFieldLabel) {

                // clear any lattice field that has been remapped
                if (stdFieldMapping.mappedField) {
                    var mappedMapping = mappedFieldMappingsMap[stdFieldMapping.mappedField];
                    if (mappedMapping) {
                        mappedMapping.mappedField = null;
                        mappedMapping.mappedToLatticeField = false;
                    }
                }

                // update user fields that has been mapped
                var userMapping = userFieldMappingsMap[stdFieldMapping.userField];
                if (userMapping) {
                    userMapping.mappedField = stdFieldMapping.mappedField;
                    userMapping.fieldType = stdFieldMapping.fieldType;
                    userMapping.mappedToLatticeField = true;
                }
            }
        }
    };

    vm.clickRemap = function() {
        $anchorScroll();

        vm.NextClicked = false;
    };

    vm.clickNextModel = function() {

        ShowSpinner('Saving Field Mappings...');

        // build ignoredFields list from temp 'ignored' fieldMapping property
        vm.fieldMappings.forEach(function(fieldMapping) {
            if (fieldMapping.ignored) {
                vm.ignoredFields.push(fieldMapping.userField);

                delete fieldMapping.ignored;
            }
        });

        ImportService.SaveFieldDocuments(vm.csvFileName, FieldDocument).then(function(result) {
            ShowSpinner('Executing Modeling Job...');

            ImportService.StartModeling(vm.csvMetadata).then(function(result) {
                if (result.Result && result.Result != "") {
                    $timeout(function() {
                        $state.go('home.models.import.job', { applicationId: result.Result });
                    }, 0);
                }
            });
        });
    };

    vm.validateMappingSelect = function(mapping) {
        var name = 'mapping_lattice_field_select_';

        vm.validateIsDuplicate(name, mapping);
    };

    vm.validateMappingInput = function(mapping) {
        var name = 'mapping_custom_field_input_';

        vm.validateIsReserved(name, mapping);
        vm.validateIsDuplicate(name, mapping);
    };

    vm.validateIsReserved = function(name, mapping) {
        var isReserved = !!vm.UnmappedFieldsMap[mapping.mappedField] && !mapping.ignored;

        if ($scope.fieldMappingForm[name + mapping.userField]) {
            $scope.fieldMappingForm[name + mapping.userField].$setValidity("Reserved", !isReserved);
        }
    };

    vm.validateIsDuplicate = function(name, mapping) {
        var value = mapping.mappedField;
        var isDuplicate = false;

        if (!mapping.ignored) {
            vm.fieldMappings.forEach(function(field) {
                if (field.mappedField == value && !field.ignored) {
                    if (mapping.userField != field.userField) {
                        isDuplicate = true;
                    }
                }
            });
        }

        if ($scope.fieldMappingForm[name + mapping.userField]) {
            $scope.fieldMappingForm[name + mapping.userField].$setValidity("Duplicate", !isDuplicate);
        }
    };

    vm.validateRequiredFields = function() {
        for (var requiredField in vm.requiredFieldsMissing) {
            var fieldMapping = vm.standardFieldMappings[requiredField];

            if (fieldMapping && fieldMapping.userField && fieldMapping.userField !== vm.ignoredFieldLabel) {
                vm.requiredFieldsMissing[requiredField] = false;
            } else {
                vm.requiredFieldsMissing[requiredField] = true;
            }
        }

        if (vm.fuzzyMatchEnabled) {
            if (vm.schema === 'SalesforceAccount') {
                if (!vm.requiredFieldsMissing['Website']) {
                    vm.requiredFieldsMissing['CompanyName'] = false;
                } else if (!vm.requiredFieldsMissing['CompanyName']) {
                    vm.requiredFieldsMissing['Website'] = false;
                }
            } else {
                if (!vm.requiredFieldsMissing['Email']) {
                    vm.requiredFieldsMissing['CompanyName'] = false;
                } else if (!vm.requiredFieldsMissing['CompanyName']) {
                    vm.requiredFieldsMissing['Email'] = false;
                }
            }
        }

        for (var field in vm.requiredFieldsMissing) {
             vm.FormValidated = vm.FormValidated && !vm.requiredFieldsMissing[field];
        }
    };

    // here are additional checks not covered by angular's built in form validation
    vm.validateForm = function() {
        vm.FormValidated = true;

        if (!vm.NextClicked) {
            vm.validateRequiredFields();
        } else {
            // make sure there are no empty drop-down selection
            vm.fieldMappings.forEach(function(fieldMapping) {
                if (!fieldMapping.mappedField && fieldMapping.mappedToLatticeField) {
                    vm.FormValidated = false;
                }

                if (fieldMapping.mappedToLatticeField) {
                    vm.validateMappingSelect(fieldMapping);
                } else {
                    vm.validateMappingInput(fieldMapping);
                }
            });
        }
    };

    vm.filterStandardList = function(input) {
        for (var stdField in vm.standardFieldMappings) {
            if (vm.standardFieldMappings[stdField].userField === input.userField) {
                return false;
            }
        }
        return true;
    };

    vm.init();
});
