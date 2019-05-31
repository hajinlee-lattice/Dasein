import  { underscore } from 'common/vendor.index';

angular.module('lp.import')
.service('ImportWizardStore', function($q, $state, ImportWizardService, ImportUtils, StringUtility, FeatureFlagService){
    var ImportWizardStore = this;

    this.init = function() {
        this.tooltipDateTxt = 'Date Format is important for our database to map your data correctly. This is only valid for Date Type data.';
        this.csvFileName = null;
        this.fieldDocument = null;
        this.userFieldsType = {};
        this.unmappedFields = null;
        this.accountIdState = {
            accountDedupeField: null,
            dedupeType: 'custom',
            selectedField: null,
            fields: ['Id']
        };

        this.validation = {
            ids: false,
            thirdpartyids: true,
            latticefields: false,
            matchtoaccounts: true,
            customfields: true,
            jobstatus: false,
            producthierarchy: false
        }

        this.saveObjects = {};
        this.fieldDocumentSaved = {};

        this.thirdpartyidFields = {
            fields: [],
            map: []
        };

        this.nonCustomIds = [];

        this.calendar = null;

        this.entityMatchEnabled = FeatureFlagService.FlagIsEnabled(FeatureFlagService.Flags().ENABLE_ENTITY_MATCH);
    }

    // These don't get cleared during normal .clear
    this.initTemplates = function() {
        this.templateAction = 'create-template';
        this.displayType = null;
        this.entityType = null;
        this.feedType = null;
        this.templateData = null;
        this.postBody = null;
        this.autoImport = true;
        this.importOnly = false;
    }

    this.init();
    this.initTemplates();

    this.clear = function() {
        this.init();
    }

    this.clearTemplates = function() {
        this.initTemplates();
    }

    this.wizardProgressItems = {
        "account": [
            { 
                label: 'Account IDs', 
                state: 'accounts.ids', 
                backState: 'home.importtemplates',
                nextLabel: 'Next, Add Other IDs',
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Other IDs', 
                state: 'accounts.ids.thirdpartyids', 
                nextLabel: 'Next, Map to Lattice Fields',
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'accounts.ids.thirdpartyids.latticefields', 
                nextLabel: 'Next, Add Custom Fields',
                nextFn: function(nextState) {
                    // ImportWizardStore.removeSavedDocumentFieldsFrom($state.current);
                    this.userFieldsType = {};
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Custom Fields', 
                state: 'accounts.ids.thirdpartyids.latticefields.customfields', 
                nextLabel: 'Next, Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping();
                    ImportUtils.remapTypes(ImportWizardStore.fieldDocumentSaved[$state.current.name], ImportWizardStore.userFieldsType, ImportWizardStore.getEntityType());
                    // ImportWizardStore.saveDocument(nextState, function(){
                    //     ImportWizardStore.setValidation('jobstatus', true); 
                    // });
                    ImportWizardStore.nextSaveFieldDocuments(nextState, function() {
                        ImportWizardStore.setValidation('jobstatus', true);                
                    }, true);
                }
            },{ 
                label: 'Save Template', 
                state: 'accounts.ids.thirdpartyids.latticefields.customfields.jobstatus', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    var fileName = ImportWizardStore.getCsvFileName(),
                        importOnly = ImportWizardStore.getImportOnly(),
                        autoImportData = ImportWizardStore.getAutoImport(),
                        postBody = ImportWizardStore.getPostBody();

                    ImportWizardService.templateDataIngestion(fileName, importOnly, autoImportData, postBody).then(function(){
                        $state.go(nextState); 
                    });
                }
            }
        ],
        "contact": [
            { 
                label: 'Contact IDs', 
                state: 'contacts.ids',
                backState: 'home.importtemplates',
                nextLabel: 'Next, Provide CRM/MAP IDs', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Other IDs', 
                state: 'contacts.ids.thirdpartyids', 
                nextLabel: 'Next, Map to Lattice Fields',
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'contacts.ids.thirdpartyids.latticefields', 
                nextLabel: 'Next, Map to Lattice Fields', 
                nextFn: function(nextState) {
                    this.userFieldsType = {};
                    // ImportWizardStore.removeSavedDocumentFieldsFrom($state.current);
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Match to Fields', 
                state: 'contacts.ids.thirdpartyids.latticefields.matchtoaccounts', 
                nextLabel: 'Next, Add Custom Fields', 
                nextFn: function(nextState) {
                    this.userFieldsType = {};
                    // ImportWizardStore.removeSavedDocumentFieldsFrom($state.current);
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Custom Fields', 
                state: 'contacts.ids.thirdpartyids.latticefields.matchtoaccounts.customfields', 
                nextLabel: 'Next, Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping();
                    ImportUtils.remapTypes(ImportWizardStore.fieldDocumentSaved[$state.current.name], ImportWizardStore.userFieldsType, ImportWizardStore.getEntityType());
                    // ImportWizardStore.saveDocument(nextState, function(){
                    //     ImportWizardStore.setValidation('jobstatus', true); 
                    // });
                    ImportWizardStore.nextSaveFieldDocuments(nextState, function(){
                        ImportWizardStore.setValidation('jobstatus', true);                
                    }, true);
                }
            },{ 
                label: 'Save Template', 
                state: 'contacts.ids.thirdpartyids.latticefields.matchtoaccounts.customfields.jobstatus', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    var fileName = ImportWizardStore.getCsvFileName(),
                        importOnly = ImportWizardStore.getImportOnly(),
                        autoImportData = ImportWizardStore.getAutoImport(),
                        postBody = ImportWizardStore.getPostBody();

                    ImportWizardService.templateDataIngestion(fileName, importOnly, autoImportData, postBody).then(function(){
                        $state.go(nextState); 
                    });
                }
            }
        ],
        "transaction": [
            { 
                label: 'Transaction IDs', 
                state: 'productpurchases.ids', 
                backState: 'home.importtemplates',
                nextLabel: 'Next, Map to Lattice Fields', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'productpurchases.ids.latticefields', 
                nextLabel: 'Next, Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping();
                    ImportWizardStore.nextSaveFieldDocuments(nextState, function(){
                        ImportWizardStore.setValidation('jobstatus', true);                
                    }, []);
                }
            },{ 
                label: 'Save Template', 
                state: 'productpurchases.ids.latticefields.jobstatus', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    var fileName = ImportWizardStore.getCsvFileName(),
                        importOnly = ImportWizardStore.getImportOnly(),
                        autoImportData = ImportWizardStore.getAutoImport(),
                        postBody = ImportWizardStore.getPostBody();

                    ImportWizardService.templateDataIngestion(fileName, importOnly, autoImportData, postBody).then(function(){
                        $state.go(nextState); 
                    });
                }
            }
        ],
        "product": [
            { 
                label: 'Product ID', 
                state: 'productbundles.ids', 
                backState: 'home.importtemplates',
                nextLabel: 'Next, Map to Lattice Fields', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'productbundles.ids.latticefields', 
                nextLabel: 'Next, Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping();
                    ImportWizardStore.nextSaveFieldDocuments(nextState, function(){
                        ImportWizardStore.setValidation('jobstatus', true);
                    }, []);
                }
            },{ 
                label: 'Save Template', 
                state: 'productbundles.ids.latticefields.jobstatus', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    var fileName = ImportWizardStore.getCsvFileName(),
                        importOnly = ImportWizardStore.getImportOnly(),
                        autoImportData = ImportWizardStore.getAutoImport(),
                        postBody = ImportWizardStore.getPostBody();

                    ImportWizardService.templateDataIngestion(fileName, importOnly, autoImportData, postBody).then(function(){
                        $state.go(nextState); 
                    });
                }
            }
        ],
        "producthierarchy": [
            { 
                label: 'Product Hierarchy ID', 
                state: 'producthierarchy.ids', 
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Product Hierarchy', 
                state: 'producthierarchy.ids.producthierarchy', 
                nextLabel: 'Next, Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping();
                    ImportWizardStore.nextSaveFieldDocuments(nextState, function(){
                        ImportWizardStore.setValidation('jobstatus', true);                
                    }, []);
                }
            },{ 
                label: 'Save Template', 
                state: 'producthierarchy.ids.producthierarchy.jobstatus', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    var fileName = ImportWizardStore.getCsvFileName(),
                        importOnly = ImportWizardStore.getImportOnly(),
                        autoImportData = ImportWizardStore.getAutoImport(),
                        postBody = ImportWizardStore.getPostBody();

                    ImportWizardService.templateDataIngestion(fileName, importOnly, autoImportData, postBody).then(function(){
                        $state.go(nextState); 
                    });
                }
            }
        ]
    }

    this.getWizardProgressItems = function(step) {
        return this.wizardProgressItems[(step || 'account')];
    }

    this.getValidation = function(type) {
        return this.validation[type];
    }

    this.setValidation = function(type, value) {
        this.validation[type] = value;
    }

    this.nextSaveGeneric = function(nextState) {
        $state.go(nextState);
    }

    this.nextSaveMapping = function(nextState) {
        this.saveDocumentFields($state.current.name);
        // this.saveDocumentFields(nextState);
        if(nextState){
            var copyFormer = this.getSavedDocumentCopy($state.current.name);
            this.fieldDocumentSaved[nextState] = copyFormer;
            $state.go(nextState);
        }
        // //console.log(this.fieldDocumentSaved);
    }

    /**
     * TO DO:
     * 
     */
    this.saveDocument = function(nextState, callback){
        var callback = (typeof callback === 'function' ? callback : function(){});

        ImportWizardStore.fieldDocument.fieldMappings = this.getSavedDocumentCopy($state.current.name);
        if(ImportWizardStore.fieldDocument.ignoredFields === null){
            ImportWizardStore.fieldDocument.ignoredFields = [];
        }

        ImportWizardService.SaveFieldDocuments( ImportWizardStore.getCsvFileName(), ImportWizardStore.getFieldDocument(), {
            feedType: ImportWizardStore.getFeedType(),
            entity: ImportWizardStore.getEntityType()
        }).then(callback);

        $state.go(nextState);
    };

    this.nextSaveFieldDocuments = function(nextState, callback, setIgnoredToArray) {
        var callback = (typeof callback === 'function' ? callback : function(){});

        if(setIgnoredToArray) {
            ImportWizardStore.fieldDocument.fieldMappings = this.getSavedDocumentCopy($state.current.name);
            if(ImportWizardStore.fieldDocument.ignoredFields === null){
                ImportWizardStore.fieldDocument.ignoredFields = [];
            }
        }

        ImportWizardStore.mergeFieldDocument().then(function(fieldDocument) {
            ImportWizardService.SaveFieldDocuments( ImportWizardStore.getCsvFileName(), ImportWizardStore.getFieldDocument(), {
                feedType: ImportWizardStore.getFeedType(),
                entity: ImportWizardStore.getEntityType()
            }).then(callback);
            $state.go(nextState);
        })

    }


    this.mergeFieldDocument = function(opts) {
            var deferred = $q.defer(),
                append = [],
                tmpFieldMappings = angular.copy(ImportWizardStore.fieldDocument.fieldMappings),
                segmentedTmpFieldMappings = {main: [], appended: []},
                opts = opts || {};

            opts.append = (opts.append === false ? false : true);
            opts.save = (opts.save === false ? false : true);
            opts.segment = opts.segment || false;

            for(var i in this.saveObjects) {
                var name = i,
                    items = this.saveObjects[name];
                
                if(items) {
                    items.forEach(function(item, key) {
                        var mappedItem = {},
                            _mappedItem = {},
                            newItem = {},
                            _newItem = {};

                        if(item.originalUserField) {
                            /**
                             * if it has an orginal user field, like in the ID step, find the original field mapping and unmap it and set up the new mapping to be appended
                             */
                            mappedItem = tmpFieldMappings.find(function(field) {
                                return (field.userField === item.originalUserField && field.mappedField === item.mappedField);
                            });
                            _mappedItem = angular.copy(mappedItem);

                            if(item && mappedItem && (item.userField !== mappedItem.userField || item.mappedField !== mappedItem.mappedField)) { 
                                mappedItem.mappedField = null; // if it's different, unmap it here
                                mappedItem.mappedToLatticeField = false;
                                if(item.append) { // and set it up to be append to fieldMappings later
                                    _mappedItem.userField = item.userField;
                                    _mappedItem.mappedField = item.mappedField;
                                    append.push(_mappedItem);
                                }
                            }
                        } else if(item.append) {
                            /**
                             * if it's new item, such as a third party id, append it
                             */
                            newItem = underscore.findWhere(tmpFieldMappings, {userField: item.userField});
                            _newItem = angular.copy(newItem);

                            var __newItem = angular.extend(_newItem, item);
                            append.push(__newItem);
                        } 
                        // This part is done in this.saveDocumentFields that is called as soon as the next button is pressed in the wizard
                        // else {
                        //     /**
                        //      * here we update fields instead of appending them.
                        //      */
                        //     var unMappedItem = tmpFieldMappings.find(function(field) {
                        //         return (field.mappedField === item.mappedField);
                        //     });
                        //     mappedItem = tmpFieldMappings.find(function(field) {
                        //         return (field.userField === item.userField);
                        //     });
                        //     if(unMappedItem & !unMappedItem.mappedToLatticeField) {
                        //         unMappedItem.mappedField = null;
                        //         unMappedItem.mappedToLatticeField = false;
                        //     }
                        //     if(mappedItem && !mappedItem.unmap) {
                        //         mappedItem.mappedField = item.mappedField;
                        //         mappedItem.mappedToLatticeField = true;
                        //     }

                        // }
                    });
                }
            }

            if(opts.segment) {
                segmentedTmpFieldMappings['main'] = tmpFieldMappings;
                segmentedTmpFieldMappings['appended'] = append;
            }

            
            if(opts.append) {
                let copyAppend = [];
                Object.keys(append).forEach( index => {
                    let userFieldName = append[index].userField;
                    let mappedFieldName = append[index].mappedField;
                    let found = underscore.findWhere(tmpFieldMappings, {userField: userFieldName, mappedField: mappedFieldName });
                    if(!found){
                        copyAppend.push(append[index]);
                    }
                });
                tmpFieldMappings = tmpFieldMappings.concat(copyAppend); // append
            }

            if(opts.save) {
                ImportWizardStore.fieldDocument.fieldMappings = tmpFieldMappings;
            }

            if(opts.segment) {
                deferred.resolve(segmentedTmpFieldMappings);
            } else {
                deferred.resolve(tmpFieldMappings);
            }
            return deferred.promise;
    }

    this.getAccountIdState = function() {
        return this.accountIdState;
    };

    this.setAccountIdState = function(nextState) {
        for (var key in this.accountIdState) {
            this.accountIdState[key] = nextState[key];
        }
    };

    this.getCsvFileName = function() {
        return this.csvFileName;
    };
    this.setCsvFileName = function(fileName) {
        this.csvFileName = fileName;
    };

    this.setPostBody = function(postBody) {
        this.postBody = postBody;
    }
    this.getPostBody = function() {
        return this.postBody;
    }

    this.setImportOnly = function(importOnly) {
        this.importOnly = importOnly;
    }
    this.getImportOnly = function() {
        // //console.log(this.importOnly);
        return this.importOnly;
    }

    this.setFieldDocument = function(data) {
        this.fieldDocument = data;
    };
    this.getFieldDocument = function() {
        return angular.copy(this.fieldDocument);
    };
    this.getFieldDocumentAttr = function(name) {
        if(name == 'fieldMappings') {
            return this.fieldDocument.fieldMappings;
        } else if(name == 'ignoredFields') {
            return this.fieldDocument.ignoredFields;
        }
    };

    this.getUnmappedFields = function() {
        return this.unmappedFields;
    };
    this.setUnmappedFields = function(data) {
        this.unmappedFields = data;
    };

    this.setTemplateAction = function(templateAction) {
        this.templateAction = templateAction;
    };
    this.getTemplateAction = function() {
        return this.templateAction;
    };

    this.setTemplateData = function(templateData) {
        this.templateData = templateData;
    };
    this.getTemplateData = function() {
        return this.templateData;
    };

    this.setDisplayType = function(displayType) {
        this.displayType = displayType;
    };
    this.getDisplayType = function() {
        return this.displayType;
    };

    this.setEntityType = function(type) {
        this.entityType = type;
    };
    this.getEntityType = function() {
        return this.entityType;
    };

    this.getFeedType = function() {
        return this.feedType;
    };
    this.setFeedType = function(type) {
        this.feedType = type;
    };

    this.getAutoImport = function(){
        return this.autoImport;
    }

    this.setAutoImport = function(importStatus){
        this.autoImport = importStatus;
    }

    this.getCustomFields = function(type) {
        var data = [],
            total = 7, 
            types = ['Text', 'Number', 'Boolean', 'Date'];
        for(var i=0;i<total;i++) {
            var tmp = {
                CustomField: 'CustomField' + (i + 1),
                Type: types, 
                Ignore: false
            };
            data.push(tmp);
        }
        return data;
    }

    this.updateSavedObjects = (object) => {
        let name = $state.current.name;
        let saved = this.saveObjects[name];
        Object.keys(saved).forEach(num => {
            if(saved[num].mappedField === object.name){
                ImportUtils.updateLatticeDateField(saved[num], object);
                return;
            }
        });
        this.setSaveObjects(saved);
        // //console.log(this.saveObjects);
         
    }

    this.setSaveObjects = function(object, key) {
        var key = key || $state.current.name || 'unknown';
        this.saveObjects[key] = object;
    };

    function getFormerState(state){
        var period = state.lastIndexOf('.');
        var formerState = state.substring(0, period);
        return formerState;
    }
    this.saveDocumentFields = function(state) {
        //console.log('Saved OBJ ', this.saveObjects);
        if(this.saveObjects[state]){
            var copy = this.getSavedDocumentCopy(getFormerState(state));// this.getFieldDocument(true).fieldMappings;
            copy = ImportUtils.updateDocumentMapping(ImportWizardStore.getEntityType(), this.saveObjects[state], copy);
            if(copy){
                this.fieldDocumentSaved[state] = copy;
                //console.log('OBJECT SAVED (if)', copy);
            }
        }else{
            var period = state.lastIndexOf('.');
            var formerState = state.substring(0, period);
            var copyDoc = this.getSavedDocumentCopy(formerState);
            if(copyDoc){
                this.fieldDocumentSaved[state] = copyDoc;
                //console.log('OBJECT SAVED (else)', copyDoc);
            }
        }
    };

    this.getSavedDocumentCopy = function(state){
        var copy = angular.copy(this.fieldDocumentSaved[state]);
        return copy;
    };

    this.setSavedDocumentInState = function(state, fieldDocumentSaved){
        this.fieldDocumentSaved[state] = fieldDocumentSaved;
    };

    this.getSavedDocumentFields = function(state){
        return this.fieldDocumentSaved[state];
    };
    

    this.removeSavedObjectFrom = function(state){
        var keys = Object.keys(this.saveObjects);
        if(keys){
            keys.forEach(function(key){
                if(key.includes(state)){
                    delete ImportWizardStore.saveObjects[key];
                }
            });
        }
    };

    this.removeSavedDocumentFields = function(state){
        delete this.fieldDocumentSaved[state];
    };
    
    this.removeSavedDocumentFieldsFrom = function(state){
        var keys = Object.keys(this.fieldDocumentSaved);
        if(keys){
            keys.forEach(function(key){
                if(key.includes(state)){
                    delete ImportWizardStore.fieldDocumentSaved[key];
                }
            });
        }
    };

    this.removeFromState = function(state){
        ImportWizardStore.removeSavedObjectFrom(state);
        ImportWizardStore.removeSavedDocumentFieldsFrom(state);
    };


    this.getSaveObjects = function(key) {
        if(key) {
            return this.saveObjects[key];
        }
        return this.saveObjects;
    }
    
    this.setThirdpartyidFields = function(fields, map) {
        this.thirdpartyidFields = {
            fields: fields,
            map: map
        };
    }

    this.getThirdpartyidFields = function() {
        return this.thirdpartyidFields;
    }

    var findIndexes = function(object, property, value) {
        var indexes = [];
        object.forEach(function(item, index) {
            if(item[property] === value) {
                indexes.push(index);
            }
        });
        return indexes;
    }

    this.remapMap = function(userField, mappedField, cdlExternalSystemType, unmap) {
        var _mappedIndexes = findIndexes(this.fieldDocument.fieldMappings, 'mappedField', mappedField), 
            userIndexes = findIndexes(this.fieldDocument.fieldMappings, 'userField', userField),
            unmappedIndexes = (unmap ? findIndexes(this.fieldDocument.fieldMappings, 'mappedField', userField) : []), // find unmapped items
            mappedIndexes = _mappedIndexes.concat(unmappedIndexes); // add unmapped items so they get same unmapping as duplciates

        mappedIndexes.forEach(function(index) { // this unmaps previous fields, to remove duplicate mappings
            ImportWizardStore.fieldDocument.fieldMappings[index].mappedField = null;
            ImportWizardStore.fieldDocument.fieldMappings[index].mappedToLatticeField = false;
        });
        userIndexes.forEach(function(index) { // this maps the new fields
            ImportWizardStore.fieldDocument.fieldMappings[index].mappedField = mappedField;
            ImportWizardStore.fieldDocument.fieldMappings[index].cdlExternalSystemType = cdlExternalSystemType;
            ImportWizardStore.fieldDocument.fieldMappings[index].mappedToLatticeField = (mappedField ? true : false); // allows for unmapping
        });
    }

    this.remapType = function(userField, type) {
        var userIndexes = findIndexes(this.fieldDocument.fieldMappings, 'userField', userField);

        userIndexes.forEach(function(index) {
            ImportWizardStore.fieldDocument.fieldMappings[index].fieldType = type;
        });
    }

    this.setIgnore = function(ignoredFields) {
        ImportWizardStore.fieldDocument.ignoredFields = ignoredFields;
    }

    this.addNonCustomIds = function(ids) {
        if(typeof ids === 'object') {
            for(var i in ids) {
                var id = ids[i];
                if(this.nonCustomIds.indexOf(id) === -1) {
                    this.nonCustomIds.push(id);
                }
            }
        } else if (typeof ids === 'string') {
            var id = ids;
            if(this.nonCustomIds.indexOf(id) === -1) {
                this.nonCustomIds.push(id);
            }
        }
    }

    this.getNonCustomIds = function() {
        return this.nonCustomIds;
    }

    this.getCalendar = function() {
        var deferred = $q.defer();

        if(this.calendar) {
            deferred.resolve(this.calendar);
        } else {
            ImportWizardService.getCalendar().then(function(result) {
                ImportWizardStore.setCalendar(result);
                deferred.resolve(result);
            });
        }
        
        return deferred.promise;
    };

    this.setCalendar = function(calendar) {
        this.calendar = calendar;
    };

    this.saveCalendar = function(calendar) {
        var deferred = $q.defer();

        ImportWizardService.saveCalendar(calendar).then(function(result) {
            ImportWizardStore.setCalendar(result);
            deferred.resolve(result);
        });
        
        return deferred.promise;
    }

    this.getCalendarInfo = function(calendar) {
        var info = {};
        if(!calendar || calendar && !Object.keys(calendar).length) {
            info.mode = 'NONE';
        } else {
            info.mode = (calendar.mode === 'STANDARD' ? 'STANDARD' : 'CUSTOM');
        }
        info.modeDisplayName = StringUtility.TitleCase(info.mode);
        return info;
    }
})
.service('ImportWizardService', function($q, $http, $state, ResourceUtility, ImportUtils, ReduxService) {

	   this.GetSchemaToLatticeFields = function(csvFileName, entity, feedType) {
	        var deferred = $q.defer();
	        var params = { 
                'entity':  entity,
                'source': 'File',
	            'feedType': feedType || '' 
            };
            if (!entity) {
                params = {};
            }


	        $http({
	            method: 'GET',
	            url: '/pls/models/uploadfile/latticeschema',
	            params: params,
	            headers: { 'Content-Type': 'application/json' }
	        }).then(
                function onSuccess(response) {

                    var result = response.data;

                    if (result != null && result !== "" && result.Success == true) {
                        //////console.log("!!!!!!!!!!!!!!!!!!!", result);

                        ImportUtils.setLatticeSchema(result.Result);
                        deferred.resolve(result.Result);

                    } else {

                        var errors = result.Errors;
                        var response = {
                                success: false,
                                errorMsg: errors[0]
                            };

                        // ////console.log("????????????????????", response);
                        deferred.resolve(response.errorMsg);
                    }

                }, function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }
                    job.status = 'Failed';

                    var errorMsg = response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

	        return deferred.promise;
	    };

	    this.GetFieldDocument = function(FileName, entity, schema, feedType) {
	        var deferred = $q.defer();
	        var entity = entity;
            var params = {};
            if (entity) {
    	       params = {
                    'entity': entity,
                    'source': 'File',
    	            'feedType': feedType || ''
                }
            } else {
                params = {
                    'schema': schema
                }
            }
            // var tmp = $state.get('home.import').data.redux;
            // tmp.fetch(FileName, entity, feedType, source);
            // //console.log(tmp);

            //console.log(params);

	        $http({
	            method: 'POST',
	            url: '/pls/models/uploadfile/' + FileName + '/fieldmappings',
	            params: params,
	            headers: { 'Content-Type': 'application/json' },
	            data: {}
	        })
	        .success(function(data, status, headers, config) {
	            if (data == null || !data.Success) {
	                if (data && data.Errors.length > 0) {
	                    var errors = data.Errors.join('\n');
                    }
	                var result = {
	                    Success: false,
	                    ResultErrors: errors || ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
	                    Result: null
	                };
	            } else {
	                var result = {
	                    Success: true,
	                    ResultErrors: data.Errors,
	                    Result: data.Result
                    };
                    if($state.get('home.import') && $state.get('home.import').data && $state.get('home.import').data.redux){
                        let redux = $state.get('home.import').data.redux;
                        redux.setInitialMapping(ImportUtils.getOriginalMapping(entity, data.Result.fieldMappings));
                    }
	            }

	            deferred.resolve(result);
	        })
	        .error(function(data, status, headers, config) {
	            var result = {
	                Success: false,
	                ResultErrors: data.errorMsg
	            };

	            deferred.resolve(result);
	        });

	        return deferred.promise;
	    };

	    this.SaveFieldDocuments = function(FileName, FieldDocument, params, forModeling) {
            var deferred = $q.defer(),
                result,
                params = params || {};

            if (!forModeling) {
                params.displayName = params.displayName || FileName,
                params.source = params.source || 'File',
                params.entity = params.entity || 'Account',
                params.feedType = params.feedType || '';
            } else {
                params.displayName = FileName;
                params.excludeCustomFileAttributes = params.excludeCustomFileAttributes;
            }

	        $http({
	            method: 'POST',
	            url: '/pls/models/uploadfile/fieldmappings',
	            headers: { 'Content-Type': 'application/json' },
	            params: params,
	            data: {
	                'fieldMappings': FieldDocument.fieldMappings,
	                'ignoredFields': FieldDocument.ignoredFields
	            }
	        }).then(
                function onSuccess(response) {

                    var result = response.data;
                    //console.log('RESULT ', result);
                    if (result != null && result !== "") {
                        result = response.data;
                        deferred.resolve(result);
                    } else {
                        // var errors = result.Errors;
                        // var result = {
                        //         success: false,
                        //         errorMsg: errors[0]
                        //     };
                        result = {};
                        deferred.resolve(result);
                        // deferred.resolve(result.errorMsg);
                    }

                }, function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg = response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

	        return deferred.promise;
	    };

	    this.templateDataIngestion = function(fileName, importOnly, autoImportData, postBody) {

	        var deferred = $q.defer(),
                result,
                url = importOnly ? '/pls/cdl/s3/template/import' : '/pls/cdl/s3/template',
                params = { 
                    'templateFileName':fileName,
    	            'source': 'File',
                    'importData': autoImportData,
                    'feedType': postBody.FeedType || ''
    	            // 'entity': entity
                };

            // //console.log(fileName, importOnly, autoImportData, postBody);
            let headers = {'Content-Type': 'application/json'};
            if(autoImportData){
                headers['ErrorDisplayOptions'] = '{"delay":5000}'; 
            }
	        $http({
                method: 'POST',
	            url: url,
                headers: headers,
	            params: params,
                data: postBody
	        }).then(
                function onSuccess(response) {
                    var result = response.data;
                    if (result != null && result !== "") {
                        result = response.data;
                        deferred.resolve(result);
                    } else {
                        // var errors = result.Errors;
                        // var result = {
                        //         success: false,
                        //         errorMsg: errors[0]
                        //     };
                        // deferred.resolve(result.errorMsg);
                        result = {};
                        deferred.resolve(result);
                    }

                }, function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg = response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

	        return deferred.promise;
	    };

        this.getCalendar = function() {
            var deferred = $q.defer();
            $http({
                method: 'GET',
                url: '/pls/datacollection/periods/calendar',
                headers: { 'Content-Type': 'application/json' }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    if (result != null && result !== "") {
                        result = response.data;
                        deferred.resolve(result);
                    } else {
                        // var errors = result.Errors;
                        // var result = {
                        //         success: false,
                        //         errorMsg: errors[0]
                        //     };
                        // deferred.resolve(result.errorMsg);
                        result = {};
                        deferred.resolve(result);
                    }

                }, function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg = response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        };

        this.validateCalendar = function(data) {
            var deferred = $q.defer();
            $http({
                method: 'POST',
                url: '/pls/datacollection/periods/calendar/validate',
                data: data,
                headers: { 'Content-Type': 'application/json' }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    if (result != null && result !== "") {
                        result = response.data;
                        deferred.resolve(result);
                    } else {
                        // var errors = result.Errors;
                        // var result = {
                        //         success: false,
                        //         errorMsg: errors[0]
                        //     };
                        // deferred.resolve(result.errorMsg);
                        result = {};
                        deferred.resolve(result);
                    }

                }, function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg = response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        };

        this.saveCalendar = function(data) {
            var deferred = $q.defer();
            $http({
                method: 'POST',
                url: '/pls/datacollection/periods/calendar',
                data: data,
                headers: { 'Content-Type': 'application/json' }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    if (result != null && result !== "") {
                        result = response.data;
                        deferred.resolve(result);
                    } else {
                        // var errors = result.Errors;
                        // var result = {
                        //         success: false,
                        //         errorMsg: errors[0]
                        //     };
                        // deferred.resolve(result.errorMsg);
                        result = {};
                        deferred.resolve(result);
                    }

                }, function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg = response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        };
        this.deleteCalendar = function() {
            var deferred = $q.defer();
            $http({
                method: 'DELETE',
                url: '/pls/datacollection/periods/calendar'
            }).then(
                function onSuccess(response) {
                    deferred.resolve(response);

                }, function onError(response) {
                    deferred.resolve(response);
                }
            );

            return deferred.promise;

        }
        this.getDateRange = function(year) {
            var deferred = $q.defer();
            $http({
                method: 'GET',
                url: '/pls/datacollection/periods/daterange/' + year,
                headers: { 'Content-Type': 'application/json' }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    if (result != null && result !== "") {
                        result = response.data;
                        deferred.resolve(result);
                    } else {
                        result = {};
                        deferred.resolve(result);
                    }

                }, function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg = response.data.errorMsg || 'unspecified error';
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        }
    });
