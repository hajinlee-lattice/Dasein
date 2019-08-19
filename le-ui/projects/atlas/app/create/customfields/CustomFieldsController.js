angular
.module('lp.create.import')
.controller('CustomFieldsController', function(
    $scope, $state, $stateParams, $timeout, $anchorScroll, ResourceUtility, FeatureFlagService,
    ImportService, ImportStore, FieldDocument, UnmappedFields, CancelJobModal, RatingsEngineStore
) {
    var _mappingOptions = [
        { id: 0, name: "Custom Predictor" },
        { id: 1, name: "Standard Field" },
        { id: 2, name: "Ignore this field" }
    ];

    var vm = this;

    angular.extend(vm, {
        FormValidated: true,
        ResourceUtility: ResourceUtility,
        csvFileName: $stateParams.csvFileName,
        cdlEnabled: FeatureFlagService.FlagIsEnabled(FeatureFlagService.Flags().ENABLE_CDL),
        fuzzyMatchEnabled: FeatureFlagService.FlagIsEnabled(FeatureFlagService.Flags().ENABLE_FUZZY_MATCH),
        mappingOptions: _mappingOptions.slice(),
        mappingOptionsReserved: Array.prototype.concat(_mappingOptions.slice(0,1), _mappingOptions.slice(2)),
        ignoredFields: FieldDocument.ignoredFields = [],
        fieldMappings: FieldDocument.fieldMappings,
        initialized: false,
        NextClicked: false,
        standardFieldsList: ['Event', 'Id', null, 'CompanyName', 'DUNS', 'City', 'State', 'PostalCode', 'Country'],
        standardFieldsListMap: {},
        requiredFieldsMissing: {
            'Event': true,
            'Id': true
        },
        requiredFieldsFuzzyMatching: {
            'CompanyName': true,
            'DUNS': true
        },
        ignoredFieldLabel: '-- Unmapped Field --',
        UnmappedFieldsMap: {},
        standardFieldMappings: {},
        AvailableFields: [],
        showAdditionalFieldsCDL: false
    });

    vm.init = function() {
        if (RatingsEngineStore.getCustomEventModelingType()) {
            RatingsEngineStore.setValidation("mapping", false);

            var customEventModelingType = RatingsEngineStore.getCustomEventModelingType(),
                dataStores = customEventModelingType == "LPI" ? ["CustomFileAttributes", "DataCloud"] : ["CDL", "DataCloud"];
            vm.showAdditionalFieldsCDL = dataStores.indexOf('CustomFileAttributes') >= 0;
        }
        vm.initialized = true;
        vm.csvMetadata = ImportStore.Get($stateParams.csvFileName) || {};
        vm.schema = vm.cdlEnabled ? 'SalesforceAccount' : vm.csvMetadata.schemaInterpretation || 'SalesforceLead';
        vm.UnmappedFields = UnmappedFields[vm.schema] || [];

        vm.standardFieldsList[2] = (vm.schema === 'SalesforceLead') ? 'Email' : 'Website';
        vm.requiredFieldsMissing[vm.standardFieldsList[2]] = true;

        if (vm.schema === 'SalesforceAccount') {
            vm.standardFieldsList.push('PhoneNumber');
        }
        if (vm.fuzzyMatchEnabled) {
            angular.extend(vm.requiredFieldsMissing, vm.requiredFieldsFuzzyMatching);
        }
        if (RatingsEngineStore.getCustomEventModelingType() == 'CDL') {
            console.log(vm.standardFieldsList);
            vm.standardFieldsList[1] = 'AccountId';
            delete vm.requiredFieldsMissing['Id'];
            vm.requiredFieldsMissing['AccountId'] = true;
        }

        vm.UnmappedFields.forEach(function(field) {
            vm.UnmappedFieldsMap[field.name] = field;
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

                console.log(field);
                console.log(vm.standardFieldMappings);

                vm.standardFieldMappings[field] = {
                    fieldType: vm.UnmappedFieldsMap[field] ? vm.UnmappedFieldsMap[field].fieldType : null,
                    mappedField: field,
                    mappedToLatticeField: true,
                    userField: vm.ignoredFieldLabel
                };
            }

            // creating a map for special handling of fields in standardFieldsList
            vm.standardFieldsListMap[field] = field;
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

        if (vm.cdlEnabled) {
            vm.updateFieldMappings();
        }
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
        RatingsEngineStore.setAvailableFields(vm.AvailableFields);

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

        vm.updateFieldMappings();
    };

    vm.updateFieldMappings = function() {
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
            var userField = stdFieldMapping.userField;

            if (userField && userField !== vm.ignoredFieldLabel) {
                // clear any lattice field that has been remapped
                if (stdFieldMapping.mappedField) {
                    var mappedMapping = mappedFieldMappingsMap[stdFieldMapping.mappedField];
                    if (mappedMapping) {
                        mappedMapping.mappedField = null;
                        mappedMapping.mappedToLatticeField = false;
                    }
                }

                // update user fields that has been mapped
                var userMapping = userFieldMappingsMap[userField];
                if (userMapping) {
                    userMapping.mappedField = stdFieldMapping.mappedField;
                    userMapping.fieldType = stdFieldMapping.fieldType;
                    userMapping.mappedToLatticeField = true;
                    delete userMapping.ignored;
                }
            } else if (userField && userField === vm.ignoredFieldLabel && vm.standardFieldsListMap[standardField]) {
                // if a userfield is reserved, and was unmapped, set as custom predictor
                var mappedFieldMapping = mappedFieldMappingsMap[standardField];
                if (mappedFieldMapping) {
                    mappedFieldMapping.mappedField = standardField;
                    mappedFieldMapping.mappedToLatticeField = false;
                }
            }
        }
    };

    vm.clickRemap = function() {
        $anchorScroll();

        vm.NextClicked = false;
    };

    vm.clickNextModel = function() {

        ShowSpinner('Saving Field Mappings and Executing Modeling Job...');

        // build ignoredFields list from temp 'ignored' fieldMapping property
        vm.fieldMappings.forEach(function(fieldMapping) {
            if (fieldMapping.ignored) {
                vm.ignoredFields.push(fieldMapping.userField);

                delete fieldMapping.ignored;
            }
        });

        ImportService.SaveFieldDocuments(vm.csvFileName, FieldDocument).then(function(result) {

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

        if (RatingsEngineStore.getCustomEventModelingType()) {
            RatingsEngineStore.setValidation('mapping', vm.FormValidated && $scope.fieldMappingForm.$valid);
        }
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
            var domainLikeField = (vm.schema === 'SalesforceAccount') ? 'Website' : 'Email';

            if (!vm.requiredFieldsMissing[domainLikeField]) {
                vm.requiredFieldsMissing['CompanyName'] = false;
                vm.requiredFieldsMissing['DUNS'] = false;
            } else if (!vm.requiredFieldsMissing['CompanyName']) {
                vm.requiredFieldsMissing[domainLikeField] = false;
                vm.requiredFieldsMissing['DUNS'] = false;
            } else if (!vm.requiredFieldsMissing['DUNS']) {
                vm.requiredFieldsMissing['CompanyName'] = false;
                vm.requiredFieldsMissing[domainLikeField] = false;
            }
        }

        for (var field in vm.requiredFieldsMissing) {
             vm.FormValidated = vm.FormValidated && !vm.requiredFieldsMissing[field];
        }

        if (vm.cdlEnabled) {
            RatingsEngineStore.setValidation("mapping", vm.FormValidated);
        }
    };

    // here are additional checks not covered by angular's built in form validation
    vm.validateForm = function() {
        vm.FormValidated = true;

        if (!vm.NextClicked) {
            vm.validateRequiredFields();
        } 

        if (vm.NextClicked || RatingsEngineStore.getCustomEventModelingType() == 'LPI') {
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

    vm.getModelingType = function() {
        return RatingsEngineStore.getCustomEventModelingType();
    }

    if (FieldDocument) {
        vm.init();
    }
});
