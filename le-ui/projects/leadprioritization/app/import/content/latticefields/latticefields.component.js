angular.module('lp.import.wizard.latticefields', [])
.controller('ImportWizardLatticeFields', function(
    $state, $stateParams,$timeout, $scope, ResourceUtility, ImportWizardService, ImportWizardStore, FieldDocument, UnmappedFields, Type, MatchingFields, AnalysisFields
) {
    var vm = this;

    angular.extend(vm, {
        importType: Type,
        matchingFieldsList: ['Website', 'DUNS','CompanyName', 'PhoneNumber', 'City', 'Country', 'State', 'PostalCode'],
        analysisFieldsList: ['Customer', 'AnnualRevenue', 'Industry', 'NumberOfEmployees'],
        matchingFields: MatchingFields,
        analysisFields: AnalysisFields,
        initialized: false,
        matchingFieldsListMap: {},
        analysisFieldsListMap: {},
        csvFileName: ImportWizardStore.getCsvFileName(),
        ignoredFields: FieldDocument.ignoredFields = [],
        fieldMappings: FieldDocument.fieldMappings,
        ignoredFieldLabel: '-- Unmapped Field --',
        UnmappedFieldsMap: {},
        matchingFieldMappings: {},
        analysisFieldMappings: {},
        AvailableFields: []
    });

    vm.init = function() {
        vm.initialized = true;
//        vm.schema = 'SalesforceAccount';
        vm.UnmappedFields = UnmappedFields;

        vm.UnmappedFields.forEach(function(field) {
            vm.UnmappedFieldsMap[field.name] = field;
        });

        var fieldMappingsMap = {};
        vm.fieldMappings.forEach(function(fieldMapping) {
            fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
        });

        vm.matchingFieldsList.forEach(function(field) {
            // create a copy of mapping object to preserve FieldDocument
            // FieldDocument updated when NextClicked
            if (fieldMappingsMap[field]) {
                vm.matchingFieldMappings[field] = angular.copy(fieldMappingsMap[field]);
                vm.matchingFieldMappings[field].mappedToLatticeField = true;
            } else {
                vm.matchingFieldMappings[field] = {
                    fieldType: vm.UnmappedFieldsMap[field] ? vm.UnmappedFieldsMap[field].fieldType : null,
                    mappedField: field,
                    mappedToLatticeField: true,
                    userField: vm.ignoredFieldLabel
                };
            }
            // creating a map for special handling of fields in standardFieldsList
            vm.matchingFieldsListMap[field] = field;
        });

        vm.analysisFieldsList.forEach(function(field) {
            // create a copy of mapping object to preserve FieldDocument
            // FieldDocument updated when NextClicked
            if (fieldMappingsMap[field]) {
                vm.analysisFieldMappings[field] = angular.copy(fieldMappingsMap[field]);
                vm.analysisFieldMappings[field].mappedToLatticeField = true;
            } else {
                vm.analysisFieldMappings[field] = {
                    fieldType: vm.UnmappedFieldsMap[field] ? vm.UnmappedFieldsMap[field].fieldType : null,
                    mappedField: field,
                    mappedToLatticeField: true,
                    userField: vm.ignoredFieldLabel
                };
            }
            // creating a map for special handling of fields in standardFieldsList
            vm.analysisFieldsListMap[field] = field;
        });
        vm.refreshLatticeFields();
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

            for (var matchingFieldMap in vm.matchingFieldMappings) {
                var fieldMapping = vm.matchingFieldMappings[matchingFieldMap];

                var userField = fieldMapping.userField;
                usedUserField[userField] = true;
            }
            for (var analysisFieldMap in vm.analysisFieldMappings) {
                var fieldMapping = vm.analysisFieldMappings[analysisFieldMap];

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

        ImportWizardStore.setAvailableFields(vm.AvailableFields);
         vm.fieldMappings.forEach(function(fieldMapping) {
            if (fieldMapping.ignored) {
                vm.ignoredFields.push(fieldMapping.userField);
                delete fieldMapping.ignored;
            }
        });
         for( var i=0 ; i< vm.fieldMappings.length; i++) {
             if (vm.fieldMappings[i].mappedField == null) {
        	     vm.fieldMappings[i].mappedField = vm.fieldMappings[i].userField;
        	     vm.fieldMappings[i].mappedToLatticeField = true;
             }
         }
         FieldDocument.fieldMappings = vm.fieldMappings;
         FieldDocument.ignoredFields = vm.ignoredFields;
         ImportWizardStore.setFieldDocument(FieldDocument);
    };


    if (FieldDocument) {
        vm.init();
    }
});