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
            one: false,
            two: true,
            three: true,
            four: true,
            five: true
        }

        this.saveObjects = [];

        this.entityType = null;
    }

    this.init();

    this.wizardProgressItems = {
        "all": [
            { 
                label: 'Account ID', 
                state: 'accounts.one', 
                backState: 'home.segment.explorer.attributes',
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Other IDs', 
                state: 'accounts.one.two', 
                nextLabel: 'Next',
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'accounts.one.two.three', 
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Custom Fields', 
                state: 'accounts.one.two.three.four', 
                nextLabel: 'Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveFieldDocuments(nextState);
                }
            },{ 
                label: 'Import Data', 
                state: 'accounts.one.two.three.four.five', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    ImportWizardService.startImportCsv(ImportWizardStore.getCsvFileName());
                    $state.go(nextState); 
                }
            }
        ],
        "contacts": [
            { 
                label: 'Contact IDs', 
                state: 'contacts.one',
                backState: 'home.segment.explorer.attributes',
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'contacts.one.two', 
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Custom Fields', 
                state: 'contacts.one.two.three', 
                nextLabel: 'Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveFieldDocuments(nextState);
                }
            },{ 
                label: 'Import Data', 
                state: 'contacts.one.two.three.four', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    ImportWizardService.startImportCsv(ImportWizardStore.getCsvFileName());
                    $state.go(nextState); 
                }
            }
        ],
        "product_purchases": [
            { 
                label: 'Transaction IDs', 
                state: 'product_purchases.one', 
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'product_purchases.one.two', 
                nextLabel: 'Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveFieldDocuments(nextState);
                }
            },{ 
                label: 'Import Data', 
                state: 'product_purchases.one.two.three', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    ImportWizardService.startImportCsv(ImportWizardStore.getCsvFileName());
                    $state.go(nextState); 
                }
            }
        ],
        "product_bundles": [
            { 
                label: 'Product ID', 
                state: 'product_bundles.one', 
                nextLabel: 'Next', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveMapping(nextState);
                } 
            },{ 
                label: 'Lattice Fields', 
                state: 'product_bundles.one.two', 
                nextLabel: 'Import File', 
                nextFn: function(nextState) {
                    ImportWizardStore.nextSaveFieldDocuments(nextState);
                }
            },{ 
                label: 'Import Data', 
                state: 'product_bundles.one.two.three', 
                nextLabel: 'Done', 
                hideBack: true,
                nextFn: function(nextState) {
                    ImportWizardService.startImportCsv(ImportWizardStore.getCsvFileName());
                    $state.go(nextState); 
                }
            }
        ]
    }

    this.getWizardProgressItems = function(step) {
        return this.wizardProgressItems[(step || 'all')];
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
        this.saveObjects.forEach(function(item, index) {
            ImportWizardStore.remapMap(item.userField, item.mappedField);
        });
        $state.go(nextState);
    }

    this.nextSaveFieldDocuments = function(nextState) {
        ImportWizardService.SaveFieldDocuments( ImportWizardStore.getCsvFileName(), ImportWizardStore.getFieldDocument(), {
            entity: ImportWizardStore.getEntityType()
        });
        $state.go(nextState); 
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
            total = 7, //Math.floor(Math.random() * 10 + 1),
            types = ['Text', 'Number', 'Boolean', 'Date'];
        for(var i=0;i<total;i++) {
            var tmp = {
                CustomField: 'CustomField' + (i + 1),
                Type: types, //[Math.floor(Math.random()*types.length)],
                Ignore: false //Math.random() >= 0.5
            };
            data.push(tmp);
        }
        return data;
    }

    this.setSaveObjects = function(object) {
        this.saveObjects = object;
    }

    this.getSaveObjects = function() {
        return this.saveObjects;
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

    this.remapMap = function(userField, mappedField) {
        var mappedIndexes = findIndexes(this.fieldDocument.fieldMappings, 'mappedField', mappedField),
            userIndexes = findIndexes(this.fieldDocument.fieldMappings, 'userField', userField);

        mappedIndexes.forEach(function(index) {
            ImportWizardStore.fieldDocument.fieldMappings[index].mappedField = null;
            ImportWizardStore.fieldDocument.fieldMappings[index].mappedToLatticeField = false;
        });

        userIndexes.forEach(function(index) {
            ImportWizardStore.fieldDocument.fieldMappings[index].mappedField = mappedField;
            ImportWizardStore.fieldDocument.fieldMappings[index].mappedToLatticeField = true;
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
            params.feedType = params.feedType || 'AccountSchema';

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

	    this.startImportCsv = function(FileName) {
	        var deferred = $q.defer();
	        var result;
	        var params = { 'templateFileName':FileName ,
	            'dataFileName': FileName,
	            'source': 'File',
	            'entity': 'Account',
	            'feedType': 'AccountSchema'};

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
});
