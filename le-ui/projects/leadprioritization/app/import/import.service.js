angular.module('lp.import')
.service('ImportWizardStore', function($q, $state, ImportWizardService){
    var ImportWizardStore = this;

    this.init = function() {
        this.csvFileName = null;
        this.fieldDocument = null;
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
            customfields: true,
            jobstatus: false,
            product_hierarchy: false
        }

        this.saveObjects = {};

        this.thirdpartyidFields = {
            fields: [],
            map: []
        };

        this.entityType = null;

        this.nonCustomIds = [];

        this.calendar = null;
    }

    this.init();

    this.clear = function() {
        this.init();
    }

    this.wizardProgressItems = {
        "account": [
            { 
                label: 'Account IDs', 
                state: 'accounts.ids', 
                backState: 'home.segment.explorer.attributes',
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
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Custom Fields', 
                state: 'accounts.ids.thirdpartyids.latticefields.customfields', 
                nextLabel: 'Next, Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveFieldDocuments(nextState, function() {
                        ImportWizardStore.setValidation('jobstatus', true);                
                    });
                }
            },{ 
                label: 'Import Data', 
                state: 'accounts.ids.thirdpartyids.latticefields.customfields.jobstatus', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    ImportWizardService.startImportCsv(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(){
                        $state.go(nextState); 
                    });
                }
            }
        ],
        "contact": [
            { 
                label: 'Contact IDs', 
                state: 'contacts.ids',
                backState: 'home.segment.explorer.attributes',
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'contacts.ids.latticefields', 
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Custom Fields', 
                state: 'contacts.ids.latticefields.customfields', 
                nextLabel: 'Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveFieldDocuments(nextState, function(){
                        ImportWizardStore.setValidation('jobstatus', true);                
                    });
                }
            },{ 
                label: 'Import Data', 
                state: 'contacts.ids.latticefields.customfields.jobstatus', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    ImportWizardService.startImportCsv(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(){
                        $state.go(nextState); 
                    });
                }
            }
        ],
        "transaction": [
            { 
                label: 'Transaction IDs', 
                state: 'product_purchases.ids', 
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'product_purchases.ids.latticefields', 
                nextLabel: 'Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                    ImportWizardStore.nextSaveFieldDocuments(nextState, function(){
                        ImportWizardStore.setValidation('jobstatus', true);                
                    });
                }
            },{ 
                label: 'Import Data', 
                state: 'product_purchases.ids.latticefields.jobstatus', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    ImportWizardService.startImportCsv(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(){
                        $state.go(nextState); 
                    });
                }
            }
        ],
        "product": [
            { 
                label: 'Product ID', 
                state: 'product_bundles.ids', 
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'product_bundles.ids.latticefields', 
                nextLabel: 'Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveFieldDocuments(nextState, function(){
                        ImportWizardStore.setValidation('jobstatus', true);
                    });
                }
            },{ 
                label: 'Import Data', 
                state: 'product_bundles.ids.latticefields.jobstatus', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    ImportWizardService.startImportCsv(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(){
                        $state.go(nextState); 
                    });
                }
            }
        ],
        "product_hierarchy": [
            { 
                label: 'Product Hierarchy ID', 
                state: 'product_hierarchy.ids', 
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Product Hierarchy', 
                state: 'product_hierarchy.ids.product_hierarchy', 
                nextLabel: 'Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                    ImportWizardStore.nextSaveFieldDocuments(nextState, function(){
                        ImportWizardStore.setValidation('jobstatus', true);                
                    });
                }
            },{ 
                label: 'Import File', 
                state: 'product_hierarchy.ids.product_hierarchy.jobstatus', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    ImportWizardService.startImportCsv(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(){
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
        // this.saveObjects.forEach(function(item, index) {
        //     ImportWizardStore.remapMap(item.userField, item.mappedField, item.cdlExternalSystemType, item.unmap);
        // });
        $state.go(nextState);
    }

    this.nextSaveFieldDocuments = function(nextState, callback) {
        var callback = (typeof callback === 'function' ? callback : function(){});

        ImportWizardStore.mergeFieldDocument().then(function(fieldDocument) {
            ImportWizardService.SaveFieldDocuments( ImportWizardStore.getCsvFileName(), ImportWizardStore.getFieldDocument(), {
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
                            newitem = {},
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
                            newItem = _.findWhere(tmpFieldMappings, {userField: item.userField});
                            _newItem = angular.copy(newItem);

                            var __newItem = angular.extend(_newItem, item);

                            append.push(__newItem);
                        } else {
                            /**
                             * here we update fields instead of appending them.
                             */
                            var unMappedItem = tmpFieldMappings.find(function(field) {
                                return (field.mappedField === item.mappedField);
                            });
                            mappedItem = tmpFieldMappings.find(function(field) {
                                return (field.userField === item.userField);
                            });
                            if(unMappedItem) {
                                unMappedItem.mappedField = null;
                                unMappedItem.mappedToLatticeField = false;
                            }
                            if(mappedItem) {
                                mappedItem.mappedField = item.mappedField;
                                mappedItem.mappedToLatticeField = true;
                            }

                        }
                    });
                }
            }

            if(opts.segment) {
                segmentedTmpFieldMappings['main'] = tmpFieldMappings;
                segmentedTmpFieldMappings['appended'] = append;
            }

            if(opts.append) {
                tmpFieldMappings = tmpFieldMappings.concat(append); // append
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

    this.getFieldDocument = function() {
        return this.fieldDocument;
    };

    this.getFieldDocumentAttr = function(name) {
        if(name == 'fieldMappings') {
            return this.fieldDocument.fieldMappings;
        } else if(name == 'ignoredFields') {
            return this.fieldDocument.ignoredFields;
        }
    };

    this.setFieldDocument = function(data) {
        this.fieldDocument = data;
    };

    this.getUnmappedFields = function() {
        return this.unmappedFields;
    };

    this.setUnmappedFields = function(data) {
        this.unmappedFields = data;
    };

    this.getEntityType = function() {
        return this.entityType;
    };

    this.setEntityType = function(type) {
        this.entityType = type;
    };

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

    this.setSaveObjects = function(object, key) {
        var key = key || $state.current.name || 'unknown';
        this.saveObjects[key] = object;
    }

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
                deferred.resolve(result);
            });
        }
        
        return deferred.promise;
    };

    this.setCalendar = function(calendar) {
        this.calendar = calendar;
    };
})
.service('ImportWizardService', function($q, $http, $state, ResourceUtility) {

	this.GetSchemaToLatticeFields = function(csvFileName, entity) {
	        var deferred = $q.defer();
	        var params = { 
                'entity':  entity,
                'source': 'File',
	            'feedType': entity + 'Schema' 
            };

	        $http({
	            method: 'GET',
	            url: '/pls/models/uploadfile/latticeschema',
	            params: params,
	            headers: { 'Content-Type': 'application/json' }
	        }).then(function(data) {
	            deferred.resolve(data.data.Result);
	        });

	        return deferred.promise;
	    };

	    this.GetFieldDocument = function(FileName, entity) {
	        var deferred = $q.defer();
	        var entity = entity;
	        var params =  {
                'entity': entity,
                'source': 'File',
	            'feedType': entity + 'Schema'
            };

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
	                result = {
	                    Success: false,
	                    ResultErrors: errors || ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
	                    Result: null
	                };
	            } else {
	                result = {
	                    Success: true,
	                    ResultErrors: data.Errors,
	                    Result: data.Result
	                };
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

	    this.SaveFieldDocuments = function(FileName, FieldDocument, params) {
            var deferred = $q.defer(),
                result,
                params = params || {};

            params.displayName = params.displayName || FileName,
            params.source = params.source || 'File',
            params.entity = params.entity || 'Account',
            params.feedType = params.feedType || params.entity + 'Schema' || 'AccountSchema';

	        $http({
	            method: 'POST',
	            url: '/pls/models/uploadfile/fieldmappings',
	            headers: { 'Content-Type': 'application/json' },
	            params: params,
	            data: {
	                'fieldMappings': FieldDocument.fieldMappings,
	                'ignoredFields': FieldDocument.ignoredFields
	            }
	        })
	        .success(function(data, status, headers, config) {
	            deferred.resolve(result);
	        })
	        .error(function(data, status, headers, config) {
	            deferred.resolve(result);
	        });

	        return deferred.promise;
	    };

	    this.startImportCsv = function(FileName, entity) {
	        var deferred = $q.defer();
	        var result;
	        var params = { 
                    'templateFileName':FileName ,
    	            'dataFileName': FileName,
    	            'source': 'File',
    	            'entity': entity,
                    'feedType': entity + 'Schema' 
                };

	        $http({
	            method: 'POST',
	            url: '/pls/cdl/import/csv',
	            headers: { 'Content-Type': 'application/json' },
	            params: params,
	        })
	        .success(function(data, status, headers, config) {
	            deferred.resolve(result);
	        })
	        .error(function(data, status, headers, config) {
	            deferred.resolve(result);
	        });

	        return deferred.promise;
	    };

        this.getCalendar = function() {
            var deferred = $q.defer();
            deferred.resolve(null);
            return deferred.promise;
        };
    });
