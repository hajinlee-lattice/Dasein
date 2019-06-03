angular.module('lp.import.wizard.latticefields', [])
.controller('ImportWizardLatticeFields', function(
    $state, $stateParams,$timeout, $scope, 
    ResourceUtility, ImportWizardService, 
    ImportWizardStore, FieldDocument, 
    UnmappedFields, Type, MatchingFields, ImportUtils,
    AnalysisFields
) {
    var vm = this;
    var alreadySaved = ImportWizardStore.getSavedDocumentFields($state.current.name);
    // console.log('ALREADY SAVED ****> ',alreadySaved)
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
    // //console.log('STORE ',masterRedux);
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
    
    // //console.log('======>>>>>÷>>>>>> ', vm.fieldMappings);
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
        vm.initDateFields();
            
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

    function geUserFieldPosition(array, userField){
        let index = -1;
        array.forEach((element, i) => {
            if(element.userField == userField){
                index = i;
                return;
            }
        });
        return index;
    }

    vm.changeLatticeField = function(mapping, form, field, updateFormats) {
        var _mapping = [];
        vm.unavailableFields = [];
        vm.ignoredFieldLabel = ignoredFieldLabel;

        for(var i in mapping) {
            var item = mapping[i],
                map = makeObject(item.userField);
            // //console.log('ITEM!!!!!!!! ', item);

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
                let index = geUserFieldPosition(_mapping, map.userField);
                if(index < 0){
                    _mapping.push(map);
                }else if(map.unmap !== true){
                    
                    _mapping.push(map);
                }else{
                    console.log('ALREADY THERE NO PUSHING', index);
                }
            }

            if(field){
                if(updateFormats !== false && map.mappedField == field.name) {
                    vm.updateDateFormat(field, map);
                    vm.setFormatFromAnalysis(map);
                    map.dateFormatString =  field.dateFormatString;
                    map.timeFormatString = field.timeFormatString;
                    map.timezone = field.timezone;
                }
            }
           
        }

        if(vm.unavailableFields.length >= vm.availableFields.length) {
            vm.ignoredFieldLabel = noFieldLabel;
        }
        
        ImportWizardStore.updateSavedObjects(_mapping);
        // console.log('MAPPING &&&&&&&&&&&& ',_mapping);
        vm.checkValidDelay(form);
        
    };
    
    vm.initDateFields = () => {
        // ////console.log('@@@@@@@@@@@@@@ INIT @@@@@@@@@@@@@@@@@@@@',vm.fieldMappings, vm.analysisFieldsList);
    
        vm.analysisFieldsList.forEach(field => {
                vm.fieldMappings.forEach(obj => {
                    if(obj.mappedField == field.name){
                        field.dateFormatString = obj.dateFormatString;
                        field.timeFormatString = obj.timeFormatString;
                        field.timezone = obj.timezone;
                        return;
                    }
                });
        });
        setTimeout(() => {
            vm.analysisFieldsList.forEach(field => {
                vm.changeLatticeField(vm.fieldMapping, vm.form, field, true);
            });
        }, 0);

        ////console.log('@@@@@@@@@@@@@ >>>>>', vm.analysisFieldsList);
    }

    vm.setFormatFromAnalysis = (map) => {
        // console.log('~~~~~~~~%%%%%%%%% ', map, vm.fieldMappings);
        Object.keys(vm.fieldMappings).forEach(key => {
            let field = vm.fieldMappings[key];
            if(map.mappedField == field.mappedField){
                map.dateFormatString = field.dateFormatString;
                map.timeFormatString = field.timeFormatString;
                map.timezone = field.timezone;
                return;
            }
        });
    }
    vm.updateDateFormat = (field, map) => {
        if(map.unmap == true){
            field.dateFormatString = null;
            field.timeFormatString = null;
            field.timezone = null;
            return;
        }
        let fieldName = field.name;
        let fieldToName = '';
        if(fieldName == map.mappedField){
            fieldToName = map.userField;
        }
        vm.fieldMappings.forEach(element => {
            // console.log('UPDATED? ',element, map);
            if(element.userField == fieldToName){
                    field.dateFormatString = element.dateFormatString;
                    field.timeFormatString = element.timeFormatString;
                    field.timezone = element.timezone;
                    // console.log('UPDATED ', field);
                return;
            }
        });
        
    }

    vm.updateFormats = (formats) => {
        // console.log(formats);
        formats.field.dateFormatString = formats.dateformat;
        formats.field.timeFormatString = formats.timeformat;
        formats.field.timezone = formats.timezone;
        let alreadySaved = ImportWizardStore.getSaveObjects($state.current.name);
        let toSave;
        if(alreadySaved){
            toSave = ImportUtils.updateFormatSavedObj(alreadySaved, formats.field);
        }else{
            // mapping, form, field, updateFormats
            vm.changeLatticeField(vm.fieldMapping,vm.form);
            alreadySaved = ImportWizardStore.getSaveObjects($state.current.name);
            toSave = ImportUtils.updateFormatSavedObj(alreadySaved, formats.field);
        }
        ImportWizardStore.setSaveObjects(toSave);
        vm.checkValidDelay(vm.form);
        
    }

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
        }, 1000);
    };

    vm.checkValid = function(form) {
        // console.log(vm.form.$valid);
        if(vm.form){
            ImportWizardStore.setValidation('latticefields', vm.form.$valid);
        }
    }

    if (FieldDocument) {
        vm.init();
    }

    vm.ignoreFormats = (field) => {
        return false;
        // return field.fromExistingTemplate;
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
});