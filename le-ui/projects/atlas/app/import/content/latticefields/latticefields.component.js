angular.module('lp.import.wizard.latticefields', [])
.controller('ImportWizardLatticeFields', function(
    $state, $stateParams,$timeout, $scope, 
    ResourceUtility, ImportWizardService, 
    ImportWizardStore, FieldDocument, 
    UnmappedFields, Type, MatchingFields, 
    AnalysisFields
) {
    var vm = this;
    var alreadySaved = ImportWizardStore.getSavedDocumentFields($state.current.name);
    // console.log(alreadySaved)
    if(alreadySaved){
        FieldDocument.fieldMappings = alreadySaved;
        // vm.updateAnalysisFields();
    }
    var makeList = function(object) {
        var list = [];
        for(var i in object) {
            list.push(object[i]);
        }
        return list;
    }

    var matchingFieldsList = makeList(MatchingFields),
        analysisFieldsList = makeList(AnalysisFields),
        ignoredFieldLabel = '-- Unmapped Field --',
        noFieldLabel = '-- No Fields Available --';

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
        ignoredFieldLabel: ignoredFieldLabel,
        UnmappedFieldsMap: {},
        matchingFieldMappings: {},
        analysisFieldMappings: {},
        availableFields: [],
        unavailableFields: [],
        savedFields: ImportWizardStore.getSaveObjects($state.current.name),
        initialMapping: {},
        keyMap: {},
        fieldDateTooltip: ImportWizardStore.tooltipDateTxt
    });
    
    // console.log('======>>>>>÷>>>>>> ', vm.fieldMappings);
    vm.latticeFieldsDescription =  function(type) {
        switch (type) {
            case 'Account': return "Lattice uses several account fields that you should provide for each record, if possible. Please review the fields below to make sure everything is mapping correctly."; 
            case 'Contacts': return "Lattice uses several contact fields that you should provide for each record, if possible. Please review the fields below to make sure everything is mapping correctly.";
            case 'Transactions': return "Please provide the following attributes Lattice platform should use to analyze your customer purchase behavior.  The data provided in this file will override the existing data.";
            case 'Products': return "Please provide product bundle names Lattice platform should use aggregate your products into logical grouping that marketings and sales team would like to use to run their plays and campaigns. The data provided in this file override existing data.";
        }
        return ;
    }
    
    vm.init = function() {
        // vm.updateAnalysisFields();
        ImportWizardStore.setValidation('latticefields', false);

        vm.matchingFieldsArr = [];
        vm.matchingFields.forEach(function(matchingField) {
            vm.matchingFieldsArr.push(matchingField.name);
        });
        if(vm.savedFields) {
            vm.fieldMappings.forEach(function(fieldMapping) {
                vm.availableFields.push(fieldMapping);
            });
            vm.savedFields.forEach(function(fieldMapping) {
                if(fieldMapping.mappedField && vm.matchingFieldsArr.indexOf(fieldMapping.userField) != -1) {
                    vm.unavailableFields.push(fieldMapping.userField);
                }
                var fieldItem = vm.fieldMappings.find(function(item) {
                    return item.userField === fieldMapping.userField;
                });

                if(fieldMapping && (fieldItem && fieldItem.mappedField)) {
                    fieldItem.mappedField = fieldMapping.mappedField;
                }
            });
        } else {
            vm.fieldMappings.forEach(function(fieldMapping) {
                if(fieldMapping.mappedField && vm.matchingFieldsArr.indexOf(fieldMapping.userField) != -1) {
                    vm.unavailableFields.push(fieldMapping.userField);
                }
                vm.availableFields.push(fieldMapping);
            });
        }
       
    };
    
    vm.noDate = () => {
        let noDate = true;
        vm.analysisFields.forEach(element => {
            if(element.type == 'DATE'){
                noDate = false;
                return;
            }
        });
        return noDate;
    };
    /**
     * NOTE: The delimiter could cause a problem if the column name has : as separator 
     * @param {*} string 
     * @param {*} delimiter 
     */
    var makeObject = function(string, delimiter) {
        var delimiter = delimiter || '^/',
            string = string || '',
            pieces = string.split(delimiter);

        return {
            mappedField: pieces[0],
            userField: (pieces[1] === "" ? fallbackUserField : pieces[1]) // allows unmapping
        }
    }

    vm.ifAvailableFields = function() {
        return (vm.unavailableFields.length >= vm.availableFields.length);
    }

    vm.changeLatticeField = function(mapping, form) {
        var _mapping = [];
        vm.unavailableFields = [];
        vm.ignoredFieldLabel = ignoredFieldLabel;

        for(var i in mapping) {
            var item = mapping[i],
                map = makeObject(item.userField);

            if(!map.userField) {
                /**
                 * to unmap find the userField using the original fieldMappings object
                 */
                var fieldItem = vm.fieldMappings.find(function(item) {
                    return item.mappedField === i;
                });
                if(fieldItem && fieldItem.userField) {
                    map.userField = fieldItem.userField;
                    map.mappedField = null;
                    map.unmap = true;
                }
            }

            if(item.userField) {
                vm.unavailableFields.push(map.userField);
            }

            if(map.userField) {
                _mapping.push(map);
            }
        }

        if(vm.unavailableFields.length >= vm.availableFields.length) {
            vm.ignoredFieldLabel = noFieldLabel;
        }
        ImportWizardStore.setSaveObjects(_mapping);
        vm.checkValid(form);
    };

    vm.checkFieldsDelay = function(form) {
        $timeout(function() {
            for(var i in vm.fieldMapping) {
                var key = i,
                    userField = vm.fieldMapping[key];

                vm.keyMap[key] = userField;
                vm.initialMapping[key] = userField;

                var fieldMapping = vm.fieldMapping[i],
                    fieldObj = makeObject(fieldMapping.userField);

                vm.unavailableFields.push(fieldObj.userField);
            }
        }, 1);
    }

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

    vm.chageDateFormat = (fieldMapping) => {
        // console.log('UPDATING ==> ',fieldMapping);
        ImportWizardStore.updateSavedObjects(fieldMapping);
        vm.updateAnalysisList();
    }
    vm.updateDateFormats = (field) => {
        Object.keys(vm.fieldMappings).forEach(key =>{
            if(vm.fieldMappings[key].mappedField == field.name){
                let mapped = vm.fieldMappings[key];
                field.dateFormatString = mapped.dateFormatString ? mapped.dateFormatString : null;
                field.timeFormatString = mapped.timeFormatString ? mapped.timeFormatString : null;
                field.timezone = mapped.timezone ? mapped.timezone : null;
                return;
            }
        });
    }

    vm.updateAnalysisList = () => {
        vm.analysisFieldsList.forEach(field => {
            vm.updateDateFormats(field);
        });
        // console.log('LIST UPDTAED ', vm.analysisFieldsList);
    };

    vm.getAnalysisField = (field) => {
        if(field.type == 'DATE'){
            // console.log('======================');
            // console.log(field);
            vm.updateDateFormats(field);
            
            // console.log(field);
            // console.log('======================');
        }
        // console.log('FIELD ~~~> ',field);
        return field;
    }
    
});