angular.module('lp.import.wizard.latticefields', [])
.controller('ImportWizardLatticeFields', function(
    $state, $stateParams,$timeout, $scope, 
    ResourceUtility, ImportWizardService, ImportWizardStore, FieldDocument, UnmappedFields, Type, MatchingFields, AnalysisFields
) {
    var vm = this;

    var makeList = function(object) {
        var list = [];
        for(var i in object) {
            list.push(object[i].name);
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