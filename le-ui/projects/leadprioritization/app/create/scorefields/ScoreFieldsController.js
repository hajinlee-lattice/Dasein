angular
.module('lp.create.import')
.controller('ScoreFieldsController', function(
    $scope, $state, $stateParams, $timeout, $rootScope, ResourceUtility, ScoreLeadEnrichmentModal,
    ImportService, ImportStore, FileHeaders, FieldDocument, CancelJobModal
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
        UserFields: [],
        MappedFields: [],
        AvailableFields: [],
        FormValidated: true,
        initialized: false,
        ignoredFieldLabel: '-- Unmapped Field --'
    });

    vm.init = function() {
        vm.initialized = true;
        
        vm.refreshLatticeFields();

        FileHeaders.forEach(function(field, index) {
            if (vm.ignoredFields.indexOf(field) < 0) {
                vm.FileHeaders.push(field);
            }
        });

        console.log('scorefields', vm);
    }

    vm.refreshLatticeFields = function() {
        vm.UserFields = [];
        vm.MappedFields = [];
        vm.AvailableFields = [];

        for (var i=0; i < vm.fieldMappings.length; i++) {
            var field = vm.fieldMappings[i];

            if (field.userField) {
                vm.UserFields.push(field.userField);
            } else {
                field.userField = vm.ignoredFieldLabel;
            }

            if (field.mappedField) {
                vm.MappedFields.push(field.mappedField);
            }

        }
        
        vm.MappedFields.forEach(function(field, index) {
            if (vm.ignoredFields.indexOf(field) < 0 && 
                vm.UserFields.indexOf(field) < 0 && 
                vm.AvailableFields.indexOf(field) < 0) {
                vm.AvailableFields.push(field);
            }
        });
    }

    vm.changeLatticeField = function(mapping, field) {
        vm.refreshLatticeFields();

        $timeout(function() {
            vm.validateForm();
            $scope.$digest();
        }, 100);
    }

    vm.clickReset = function($event) {
        if ($event != null) {
            $event.stopPropagation();
        }

        CancelJobModal.show(null, { sref: 'home.model.scoring' });
    };

    vm.clickNext = function() {
        ShowSpinner('Saving Field Mappings...');

        FieldDocument.fieldMappings = vm.fieldMappings.filter(function(field) { return field.userField != vm.ignoredFieldLabel });
        
        ImportService.SaveFieldDocuments(vm.csvFileName, FieldDocument, true).then(function(result) {
            ShowSpinner('Preparing Scoring Job...');
            ScoreLeadEnrichmentModal.showFileScoreModal(vm.modelId, vm.csvFileName);
        });
    }

    vm.validateForm = function() {
        vm.FormValidated = true;

        vm.fieldMappings.forEach(function(fieldMapping, index) {
            if (vm.requiredFields.indexOf(fieldMapping.mappedField) > -1 && (!fieldMapping.userField || fieldMapping.userField == vm.ignoredFieldLabel)) {
                vm.FormValidated = false;
            }
        });
    }

    vm.init();
});
