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
        ignoredFields: FieldDocument.ignoredFields,
        fieldMappings: FieldDocument.fieldMappings,
        requiredFields: FieldDocument.requiredFields,
        FileHeaders: [],
        FormValidated: true,
        initialized: false,
        NextClicked: false,
        ignoredFieldLabel: '-- Unmapped Field --'
    });

    vm.init = function() {
        vm.initialized = true;
''
        FileHeaders.forEach(function(field, index) {
            if (vm.ignoredFields.indexOf(field) < 0) {
                vm.FileHeaders.push(field);
            }
        });

        vm.refreshLatticeFields();

        vm.validateForm();
    }

    vm.refreshLatticeFields = function(current) {
        vm.FieldMap = {};
        vm.UserMap = {};
        vm.FileFields = [];
        vm.ModelFields = [];
        vm.AvailableFields = [];

        vm.fieldMappings.forEach(function(mapping, index) {
            if (current) {
                if (mapping.mappedField != current.mappedField && mapping.userField == current.userField) {
                    mapping.userField = vm.ignoredFieldLabel;
                }
            }

            if (mapping.userField && mapping.userField != vm.ignoredFieldLabel) {
                vm.UserMap[mapping.userField] = mapping;
                vm.FileFields.push(mapping.userField);
            } else {
                mapping.userField = vm.ignoredFieldLabel;
            }

            if (mapping.mappedField) {
                vm.FieldMap[mapping.mappedField] = mapping;
                vm.ModelFields.push(mapping.mappedField);
            }
        });
        
        vm.FileHeaders.forEach(function(field, index) {
            var mapping = vm.UserMap[field];
            
            console.log(index, field, mapping);
            
            if (!mapping || !isIn(mapping.mappedField, vm.requiredFields)) {
                vm.AvailableFields.push(field);
            }
        });
    }

    var isIn = function(item, array) {
        return array.indexOf(item) > -1;
    }

    vm.changeLatticeField = function(mapping, field) {
        vm.refreshLatticeFields(mapping);

        $timeout(function() {
            vm.validateForm();
        }, 100);
    }

    vm.clickReset = function($event) {
        if ($event != null) {
            $event.stopPropagation();
        }

        CancelJobModal.show(null, { sref: 'home.model.jobs' });
    };

    vm.clickNext = function() {
        $anchorScroll();

        ShowSpinner('Saving Field Mappings...');

        FieldDocument.fieldMappings = vm.fieldMappings.filter(function(field) { return field.userField != vm.ignoredFieldLabel });
        
        ImportService.SaveFieldDocuments(vm.csvFileName, FieldDocument, true).then(function(result) {
            ShowSpinner('Preparing Scoring Job...');
            ScoreLeadEnrichmentModal.showFileScoreModal(vm.modelId, vm.csvFileName, 'home.model.scoring');
        });
    }

    vm.validateForm = function() {
        vm.FormValidated = true;

        vm.fieldMappings.forEach(function(fieldMapping, index) {
            if (isIn(fieldMapping.mappedField, vm.requiredFields) && (!fieldMapping.userField || fieldMapping.userField == vm.ignoredFieldLabel)) {
                vm.FormValidated = false;
            }
        });
    }

    vm.init();
});
