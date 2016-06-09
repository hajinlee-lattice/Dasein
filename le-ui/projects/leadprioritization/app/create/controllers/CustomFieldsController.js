angular.module('mainApp.create.customFields', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.create.csvImport'
])
.controller('CustomFieldsController', function($state, $stateParams, ResourceUtility, csvImportService, csvImportStore, FieldDocument, UnmappedFields) {
    var vm = this;

    angular.extend(vm, {
        FormValidated: true,
        ResourceUtility : ResourceUtility,
        csvFileName: $stateParams.csvFileName,
        mappingOptions: [
            { id: 0, name: "Custom Field Name" },
            { id: 1, name: "Map to Standard Field" },
            { id: 2, name: "Ignore this field" }
        ],
        ignoredFields: FieldDocument.ignoredFields = [],
        fieldMappings: FieldDocument.fieldMappings,
        RequiredFields: []
    });

    vm.init = function() {
        vm.csvMetadata = csvImportStore.Get($stateParams.csvFileName) || {};
        vm.schema = vm.csvMetadata.schemaInterpretation;
        vm.UnmappedFields = UnmappedFields[vm.schema] || [];

        vm.UnmappedFields.forEach(function(field, index) {
            if (field.requiredType == 'Required') {
                vm.RequiredFields.push(field.name);
            }
        });

        vm.refreshLatticeFields();
    }

    vm.changeMappingOption = function(fieldMapping, selectedOption) {
        fieldMapping.mappedToLatticeField = false;
        delete fieldMapping.ignored;

        switch (selectedOption.id) {
            case 0: // custom user mapping
                fieldMapping.mappedField = fieldMapping.mappedField || fieldMapping.userField;
                break;
            case 1: // map to lattice cloud
                fieldMapping.mappedField = vm.UnmappedFieldsMap[fieldMapping.mappedField] 
                    ? fieldMapping.mappedField 
                    : '';

                fieldMapping.mappedToLatticeField = true;
                break;
            case 2: // ignore this field
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
                } else {
                    // ERROR
                }
            });
        });
    }

    // here are additional checks not covered by angular's built in form validation
    vm.validateForm = function() {
        vm.FormValidated = true;

        // make sure there are no empty drop-down selection
        vm.fieldMappings.forEach(function(fieldMapping, index) {
            if (!fieldMapping.mappedField && fieldMapping.mappedToLatticeField) {
                vm.FormValidated = false;
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
