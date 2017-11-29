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
        ignoredFields: FieldDocument.ignoredFields = [],
        fieldMappings: FieldDocument.fieldMappings,
        ignoredFieldLabel: '-- Unmapped Field --',
        UnmappedFieldsMap: {},
        matchingFieldMappings: {},
        analysisFieldMappings: {},
        availableFields: [],
        unavailableFields: []
    });


    vm.latticeFieldsDescription =  function(type) {
        switch (type) {
            case 'Account': return "Lattice uses several account fields that you should provide for each record, if possible. Please review the fields below to make sure everything is mapping correctly."; 
            case 'Contacts': return "Lattice uses several contact fields that you should provide for each record, if possible. Please review the fields below to make sure everything is mapping correctly.";
            case 'Transactions': return "Please provde the following attributes Lattice platform should use to aggregate & match your customer perchase records to your accounts & contacts. The data provided in this file override existing data.";
            case 'Products': return "Please provide product bundle names Lattice platefor should use aggregate your products into logical grouping that marketings and sales team would like to use to run their palys and campaigns. The data provided in this file override existing data.";
        }
        return 
    }

    vm.init = function() {
        vm.fieldMappings.forEach(function(fieldMapping) {
            vm.availableFields.push(fieldMapping);
        });
    };

    var makeObject = function(pseudoObject, delimiter) {
        var delimiter = delimiter || ':',
            pieces = pseudoObject.split(delimiter);

        return {
            mappedField: pieces[0],
            userField: pieces[1]
        }
    }

    vm.changeLatticeField = function(mapping) {
        var _mapping = [];
        vm.unavailableFields = [];
        for(var i in mapping) {
            var item = mapping[i],
                 map = makeObject(item.userField);
            vm.unavailableFields.push(map.userField);
            _mapping.push(map);
        }
        ImportWizardStore.setSaveObjects(_mapping);
    };

    if (FieldDocument) {
        vm.init();
    }
});