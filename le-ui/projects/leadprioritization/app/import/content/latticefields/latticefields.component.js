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
            if (!usedUserField[userField] && fieldMapping.mappedField != 'Id' && fieldMapping.mappedField != 'CRMId') {
                vm.AvailableFields.push(userField);
            }
        });


/*save the  fieldDocument, these should be save when click the button*/
        var userFieldMappingsMap = {},
        mappedFieldMappingsMap = {};

	    vm.fieldMappings.forEach(function(fieldMapping) {
	        userFieldMappingsMap[fieldMapping.userField] = fieldMapping;
	
	        if (fieldMapping.mappedField) {
	            mappedFieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
	        }
	    });
	
	    for (var matchingField in vm.matchingFieldMappings) {
	        var matchingFieldMapping = vm.matchingFieldMappings[matchingField];
	        var userField = matchingFieldMapping.userField;
	
	        if (userField && userField !== vm.ignoredFieldLabel) {
	            // clear any lattice field that has been remapped
	            if (matchingFieldMapping.mappedField) {
	                var mappedMapping = mappedFieldMappingsMap[matchingFieldMapping.mappedField];
	                if (mappedMapping) {
	                    mappedMapping.mappedField = null;
	                    mappedMapping.mappedToLatticeField = false;
	                }
	            }
	
	            // update user fields that has been mapped
	            var userMapping = userFieldMappingsMap[userField];
	            if (userMapping) {
	                userMapping.mappedField = matchingFieldMapping.mappedField;
	                userMapping.fieldType = matchingFieldMapping.fieldType;
	                userMapping.mappedToLatticeField = true;
	                delete userMapping.ignored;
	            }
	        } else if (userField && userField === vm.ignoredFieldLabel && vm.matchingFieldsListMap[matchingField]) {
	            // if a userfield is reserved, and was unmapped, set as custom predictor
	            var mappedFieldMapping = mappedFieldMappingsMap[matchingField];
	            if (mappedFieldMapping) {
	                mappedFieldMapping.mappedField = matchingField;
	                mappedFieldMapping.mappedToLatticeField = false;
	            }
	        }
	    }

	    for (var analysisField in vm.analysisFieldMappings) {
	        var analysisFieldMapping = vm.analysisFieldMappings[analysisField];
	        var userField = analysisFieldMapping.userField;
	
	        if (userField && userField !== vm.ignoredFieldLabel) {
	            // clear any lattice field that has been remapped
	            if (analysisFieldMapping.mappedField) {
	                var mappedMapping = mappedFieldMappingsMap[analysisFieldMapping.mappedField];
	                if (mappedMapping) {
	                    mappedMapping.mappedField = null;
	                    mappedMapping.mappedToLatticeField = false;
	                }
	            }
	
	            // update user fields that has been mapped
	            var userMapping = userFieldMappingsMap[userField];
	            if (userMapping) {
	                userMapping.mappedField = analysisFieldMapping.mappedField;
	                userMapping.fieldType = analysisFieldMapping.fieldType;
	                userMapping.mappedToLatticeField = true;
	                delete userMapping.ignored;
	            }
	        } else if (userField && userField === vm.ignoredFieldLabel && vm.analysisFieldsListMap[analysisField]) {
	            // if a userfield is reserved, and was unmapped, set as custom predictor
	            var mappedFieldMapping = mappedFieldMappingsMap[analysisField];
	            if (mappedFieldMapping) {
	                mappedFieldMapping.mappedField = analysisField;
	                mappedFieldMapping.mappedToLatticeField = false;
	            }
	        }
	    }
        ImportWizardStore.setFieldDocument(FieldDocument);
    };


    if (FieldDocument) {
        vm.init();
    }
});