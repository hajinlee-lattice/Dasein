angular
.module('lp.create.import')
.controller('ScoreFieldsController', function(
    $scope, $state, $stateParams, $timeout, $rootScope, $anchorScroll, ResourceUtility,
    ScoreLeadEnrichmentModal, ImportService, ImportStore, FileHeaders, FieldDocument, CancelJobModal
) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        modelId: $stateParams.modelId,
        csvFileName: $stateParams.csvFileName,
        standardFieldsList: ['Id', 'Email', 'CompanyName', 'State', 'Zip', 'Country', 'PhoneNumber'],
        requiredFieldsMissing: {
            'Id': true,
            'Email': true,
            'CompanyName': true
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
        var fieldMappingsMap = {};

        FieldDocument.fieldMappings.forEach(function(fieldMapping) {
            fieldMappingsMap[fieldMapping.userField] = fieldMapping;
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

        FieldDocument.fieldMappings.forEach(function (fieldMapping) {
            if (!vm.standardFieldMappings[fieldMapping.userField]) {
                vm.additionalFieldMappings[fieldMapping.userField] = fieldMappingsMap[fieldMapping.userField];
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
            for (var additionalField in vm.additionalFieldMappings) {
                var mapping = vm.additionalFieldMappings[additionalField];
                if ((mapping.mappedField !== current.mappedField &&
                    mapping.userField === current.userField) ||
                    mappedSet[mapping.userField]) {
                    mapping.userField = vm.ignoredFieldLabel;
                }

            }
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
    };

    vm.clickNextScore = function() {
        $anchorScroll();

        ShowSpinner('Saving Field Mappings...');

        FieldDocument.fieldMappings = _.chain(angular.extend({}, vm.standardFieldsMapping, vm.additionalFieldMappings))
            .pick(function (item) {
                return item.userField !== vm.ignoredFieldLabel;
            }).values().value();

        ImportService.SaveFieldDocuments(vm.csvFileName, FieldDocument, true).then(function(result) {
            ShowSpinner('Preparing Scoring Job...');
            ScoreLeadEnrichmentModal.showFileScoreModal(vm.modelId, vm.csvFileName, 'home.model.jobs');
        });
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

        if (!vm.requiredFieldsMissing['Email']) {
            vm.requiredFieldsMissing['CompanyName'] = false;
        } else if (!vm.requiredFieldsMissing['CompanyName']) {
            vm.requiredFieldsMissing['Email'] = false;
        }

        for (var field in vm.requiredFieldsMissing) {
             vm.FormValidated = vm.FormValidated && !vm.requiredFieldsMissing[field];
        }
    };

    vm.init();
});
