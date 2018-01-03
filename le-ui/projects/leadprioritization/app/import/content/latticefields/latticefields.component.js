angular.module('lp.import.wizard.latticefields', [])
.controller('ImportWizardLatticeFields', function(
    $state, $stateParams,$timeout, $scope, 
    ResourceUtility, ImportWizardService, ImportWizardStore, FieldDocument, UnmappedFields, Type, MatchingFields, AnalysisFields
) {
    var vm = this;

    var makeList = function(object) {
        var list = [];
        for(var i in object) {
            list.push(object[i]);
        }
        return list;
    }

    var matchingFieldsList = makeList(MatchingFields),
        analysisFieldsList = makeList(AnalysisFields);

    angular.extend(vm, {
        importType: Type,
        matchingFieldsList: angular.copy(matchingFieldsList),
        analysisFieldsList: angular.copy(analysisFieldsList),
        matchingFields: MatchingFields,
        analysisFields: AnalysisFields,
        initialized: false,
        matchingFieldsListMap: {},
        analysisFieldsListMap: {},
        csvFileName: ImportWizardStore.getCsvFileName(),
        ignoredFields: FieldDocument.ignoredFields || [],
        fieldMappings: FieldDocument.fieldMappings,
        ignoredFieldLabel: '-- Unmapped Field --',
        UnmappedFieldsMap: {},
        matchingFieldMappings: {},
        analysisFieldMappings: {},
        availableFields: [],
        unavailableFields: [],
    });


    vm.latticeFieldsDescription =  function(type) {
        switch (type) {
            case 'Account': return "Lattice uses several account fields that you should provide for each record, if possible. Please review the fields below to make sure everything is mapping correctly."; 
            case 'Contacts': return "Lattice uses several contact fields that you should provide for each record, if possible. Please review the fields below to make sure everything is mapping correctly.";
            case 'Transactions': return "Please provide the following attributes Lattice platform should use to analyze your customer purchase behavior.  The data provided in this file will override the existing data.";
            case 'Products': return "Please provide product bundle names Lattice platform should use aggregate your products into logical grouping that marketings and sales team would like to use to run their plays and campaigns. The data provided in this file override existing data.";
        }
        return 
    }

    vm.init = function() {
        ImportWizardStore.setValidation('latticefields', false);

        vm.matchingFieldsArr = [];
        vm.matchingFields.forEach(function(matchingField) {
            vm.matchingFieldsArr.push(matchingField.name);
        });

        vm.fieldMappings.forEach(function(fieldMapping) {
            if(fieldMapping.mappedField && vm.matchingFieldsArr.indexOf(fieldMapping.userField) != -1) {
                vm.unavailableFields.push(fieldMapping.userField);
            }
            vm.availableFields.push(fieldMapping);
        });
    };

    var makeObject = function(string, delimiter) {
        var delimiter = delimiter || ':',
            string = string || '',
            pieces = string.split(delimiter);

        return {
            mappedField: pieces[0],
            userField: (pieces[1] === "" ? fallbackUserField : pieces[1])// allows unmapping
        }
    }

    vm.ifAvailableFields = function() {
        return (vm.unavailableFields.length >= vm.availableFields.length);
    }

    vm.changeLatticeField = function(mapping, form) {
        var _mapping = [];
        vm.unavailableFields = [];
        for(var i in mapping) {
            var item = mapping[i],
                map = makeObject(item.userField);

            if(!map.userField) {
                map.userField = i;
                map.mappedField = null;
            }
            if(item.userField) {
                vm.unavailableFields.push(map.userField);
            }
            _mapping.push(map);
        }
        ImportWizardStore.setSaveObjects(_mapping);
        vm.checkValid(form);
    };

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        ImportWizardStore.setValidation('latticefields', form.$valid);
    }

    if (FieldDocument) {
        vm.init();
    }
});