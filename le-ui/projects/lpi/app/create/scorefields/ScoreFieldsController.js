angular
.module('lp.create.import')
.controller('ScoreFieldsController', function(
    $scope, $state, $stateParams, $timeout, $rootScope, $anchorScroll, ResourceUtility, FeatureFlagService,
    ScoreLeadEnrichmentModal, ImportService, ImportStore, FileHeaders, FieldDocument, CancelJobModal, Model
) {

    var vm = this;
    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        modelId: $stateParams.modelId,
        csvFileName: $stateParams.csvFileName,
        schema: Model.ModelDetails.SourceSchemaInterpretation,
        useFuzzyMatch: false,
        standardFieldsList: ['Id', null, 'CompanyName', 'DUNS', 'City', 'State', 'PostalCode', 'Country'],
        requiredFieldsMissing: {
            'Id': true
        },
        requiredFieldsFuzzyMatching: {
            'CompanyName': true,
            'DUNS': true
        },
        standardFieldMappings: {},
        additionalFieldMappings: {},
        initialized: false,
        FormValidated: true,
        NextClicked: false,
        ignoredFieldLabel: '-- Unmapped Field --'
    });

    vm.init = function() {
        vm.initialized = true;

        vm.standardFieldsList[1] = (vm.schema === 'SalesforceAccount') ? 'Website' : 'Email';
        vm.requiredFieldsMissing[vm.standardFieldsList[1]] = true;

        var fuzzyMatchEnabled = FeatureFlagService.FlagIsEnabled(FeatureFlagService.Flags().ENABLE_FUZZY_MATCH);
        var isRTS = Model.EventTableProvenance &&
                    (!Model.EventTableProvenance.hasOwnProperty('Data_Cloud_Version') ||
                    Model.EventTableProvenance.Data_Cloud_Version === null ||
                    Model.EventTableProvenance.Data_Cloud_Version.substring(0,2) === '1.');
        vm.useFuzzyMatch = fuzzyMatchEnabled && !isRTS;

        if (vm.useFuzzyMatch) {
            angular.extend(vm.requiredFieldsMissing, vm.requiredFieldsFuzzyMatching);
        }

        if (vm.schema === 'SalesforceAccount') {
            vm.standardFieldsList.push('PhoneNumber');
        }
        var fieldMappingsMap = {};
        FieldDocument.fieldMappings.forEach(function(fieldMapping) {
            if (fieldMapping.mappedField) {
                fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
            }

            if (!fieldMapping.userField && fieldMapping.mappedField) {
                fieldMapping.userField = vm.ignoredFieldLabel;
            }
        });

        vm.standardFieldsList.forEach(function(field) {
            if (fieldMappingsMap[field]) {
                vm.standardFieldMappings[field] = fieldMappingsMap[field];
            } else {
                vm.standardFieldMappings[field] = {
                    fieldType: null,
                    mappedField: field,
                    mappedToLatticeField: true,
                    userField: vm.ignoredFieldLabel
                };
            }
        });

        vm.refreshFields();

        vm.validateForm();
    }

    vm.refreshFields = function(current) {
        vm.AvailableFields = [];
        var mappedSet = {};

        for (var standardField in vm.standardFieldMappings) {
            var mapping = vm.standardFieldMappings[standardField];

            if (mapping.userField && mapping.userField !== vm.ignoredFieldLabel) {
                mappedSet[mapping.userField] = true;
            }
        }

        if (current) {
            FieldDocument.fieldMappings.forEach(function(mapping) {
                if (mapping.mappedField !== current.mappedField &&
                    mapping.userField === current.userField) {
                    mapping.userField = vm.ignoredFieldLabel;
                }
            });
        }

        FileHeaders.forEach(function(userField, index) {
            if (!mappedSet[userField]) {
                vm.AvailableFields.push(userField);
            }
        });
    }

    vm.changeField = function(mapping) {
        vm.refreshFields(mapping);

        $timeout(function() {
            vm.validateForm();
        }, 100);
    };

    vm.clickRemap = function () {
        $anchorScroll();

        vm.NextClicked = false;
    };

    vm.clickCancel = function($event) {
        if ($event != null) {
            $event.stopPropagation();
        }

        CancelJobModal.show(null, { sref: 'home.model.jobs' });
    };

    vm.clickNext = function() {
        vm.NextClicked = true;

        var fieldMappingsMap = {};
        FieldDocument.fieldMappings.forEach(function(fieldMapping) {
            if (fieldMapping.mappedField && fieldMapping.mappedField !== vm.ignoredFieldLabel) {
                fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
            }
        });

        FieldDocument.fieldMappings.forEach(function (fieldMapping) {
            if (!vm.standardFieldMappings[fieldMapping.mappedField]) {
                if (fieldMapping.mappedField && fieldMapping.mappedField !== vm.ignoredFieldLabel) {
                    vm.additionalFieldMappings[fieldMapping.mappedField] = fieldMappingsMap[fieldMapping.mappedField];
                }
            }
        });
    };

    vm.clickNextScore = function() {
        $anchorScroll();

        ShowSpinner('Saving Field Mappings and Preparing Scoring Job...');

        FieldDocument.fieldMappings = _.chain(angular.extend({}, vm.standardFieldMappings, vm.additionalFieldMappings))
            .pick(function (item) {
                if(item.userField == vm.ignoredFieldLabel) {
                    FieldDocument.ignoredFields.push(item.mappedField);
                }
                return item.userField !== vm.ignoredFieldLabel;
            }).values().value();

        var mappedUserAttributes = _.map(FieldDocument.fieldMappings, 'userField');
        vm.AvailableFields.forEach(function(availableField) {
            if (mappedUserAttributes.indexOf(availableField) < 0) {
                FieldDocument.fieldMappings.push( { mappedField: null, mappedToLatticeField: false, userField: availableField, fieldType: null } );
            }
        })

        ImportService.SaveFieldDocuments(vm.csvFileName, FieldDocument, true).then(function(result) {
            ScoreLeadEnrichmentModal.showFileScoreModal(vm.modelId, vm.csvFileName, 'home.model.jobs');
        });

        // ImportService.SaveFieldDocuments(vm.csvFileName, FieldDocument, true).then(function(result) {

        //     ImportService.StartTestingSet(vm.modelId, vm.csvFileName, true).then(function(result) {
        //         $scope.saveInProgress = false;
        //         if (result.Success) {
        //             $state.go('home.model.jobs', { 'jobCreationSuccess': result.Success });
        //         } else {
        //             $state.go('home.model.scoring', { 'showImportError': result.Error });
        //         }
        //     });

        // });

    };

    vm.validateForm = function() {
        vm.FormValidated = true;

        for (var field in vm.requiredFieldsMissing) {
            var fieldMapping = vm.standardFieldMappings[field];
            if (!fieldMapping || fieldMapping.userField === vm.ignoredFieldLabel) {
                vm.requiredFieldsMissing[field] = true;
            } else {
                vm.requiredFieldsMissing[field] = false;
            }
        }

        if (vm.useFuzzyMatch) {
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
    };

    if (FieldDocument) {
        vm.init();
    }
});
