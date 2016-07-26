angular
.module('lp.create.import')
.service('ImportStore', function($q) {
    var ImportStore = this;
    this.files = {};
    this.FieldDocuments = {};
    this.advancedSettings = {
        oneLeadPerDomain: false,
        includePersonalEmailDomains: true,
        useLatticeAttributes: true
    };
    this.CurrentFieldMapping = null;

    this.SetAdvancedSettings = function(key, value) {
        this.advancedSettings[key] = value;
    };

    this.GetAdvancedSetting = function(key) {
        return this.advancedSettings[key];
    };

    this.Get = function(name, root) {
        return root ? this[name] : this.files[name];
    }

    this.Set = function(name, data, root) {
        if (root) {
            this[name] = data;
        } else {
            this.files[name] = data;
        }
    }

    this.GetFieldDocument = function(name) {
        var deferred = $q.defer();

        if (this.FieldDocuments[name]) {
            deferred.resolve(this.FieldDocuments[name]);
        }

        return deferred.promise;
    }

    this.SetFieldDocument = function(name, data) {
        this.FieldDocuments[name] = data;
    }
})
.service('ImportService', function($q, $http, ResourceUtility, BrowserStorageUtility, ImportStore, ServiceErrorUtility) {
    this.Upload = function(options) {
        var deferred = $q.defer(),
            formData = new FormData(),
            params = options.params || {},
            whitelist = [
                'schema','modelId','description','compressed',
                'displayName','file','metadataFile'
            ];

        if (params.metadataFile) {
            params['metadataFile'] = options.file;
        } else if (options.file) {
            params['file'] = options.file;
        }
        
        if (params.displayName) {
            params['displayName'] = params.displayName.replace('C:\\fakepath\\','');
        }

        whitelist.forEach(function(key, value) {
            if (params[key] && (params[key] != null || params[key] != undefined)) {
                formData.append(key, params[key]);
            }
        });

        // can't use $http because it does not expose onprogress event
        var xhr = new XMLHttpRequest();
        
        if (options.progress) {
            (xhr.upload || xhr).addEventListener('progress', options.progress);
        }

        xhr.addEventListener('load', function(event) {
            xhr.data = JSON.parse(this.responseText);
            ServiceErrorUtility.check(xhr);
            deferred.resolve(xhr.data);
        });

        xhr.addEventListener('error', function(event) {
            xhr.data = JSON.parse(this.responseText);
            ServiceErrorUtility.check(xhr);

            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('MODEL_IMPORT_CONNECTION_ERROR'),
                Result: null
            };

            deferred.resolve(result);
        });

        xhr.addEventListener('abort', function(event) {
            deferred.resolve(this.responseText);
        });

        xhr.open('POST', options.url);

        if (BrowserStorageUtility.getTokenDocument()) {
            xhr.setRequestHeader("Authorization", BrowserStorageUtility.getTokenDocument());
        }

        xhr.setRequestHeader("ErrorDisplayMethod", (options.ErrorDisplayMethod || 'banner'));
        //xhr.setRequestHeader("Content-Encoding", "gzip");

        ImportStore.Set('cancelXHR', xhr, true);

        xhr.send(formData);
        
        return deferred.promise;
    };

    this.GetSchemaToLatticeFields = function() {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/models/uploadfile/latticeschema',
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            deferred.resolve(data.Result);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(data.Result);
        });

        return deferred.promise;
    }

    this.GetFieldDocument = function(FileName) {
        var deferred = $q.defer();
        var schema = ImportStore.Get(FileName).schemaInterpretation;

        $http({
            method: 'GET',
            url: '/pls/models/uploadfile/' + FileName + '/fieldmappings',
            headers: { 'Content-Type': 'application/json' },
            params: { 'schema': schema }
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

    this.SaveFieldDocuments = function(FileName, FieldDocument) {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'POST',
            url: '/pls/models/uploadfile/fieldmappings',
            headers: { 'Content-Type': 'application/json' },
            params: { 'displayName': FileName },
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

    this.StartModeling = function(MetaData) {
        var deferred = $q.defer(),
            data = {
                description: MetaData.description,
                filename: MetaData.name,
                name: MetaData.modelName,
                deduplicationType: ImportStore.GetAdvancedSetting('oneLeadPerDomain') ? 'ONELEADPERDOMAIN' : 'MULTIPLELEADSPERDOMAIN',
                excludePublicDomains: ImportStore.GetAdvancedSetting('includePersonalEmailDomains') ? false : true,
                excludePropDataColumns: ImportStore.GetAdvancedSetting('useLatticeAttributes') ? false : true,
                displayName: MetaData.displayName
            };

        if (MetaData.moduleName && MetaData.pivotFileName) {
            data.moduleName = MetaData.moduleName;
            data.pivotFileName = MetaData.pivotFileName;
        }

        $http({
            method: 'POST',
            url: '/pls/models/' + MetaData.modelName,
            data: data,
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
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

    this.StartPMMLModeling = function(options) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/models/pmml/' + options.modelName,
            params: {
                'displayname': options.displayname,
                'module': options.module,
                'pmmlfile': options.pmmlfile,
                'pivotfile': options.pivotfile,
                'schema': options.schema
            },
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
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

    this.StartTestingSet = function(modelId, fileName, performEnrichment) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/scores/' + modelId,
            params: {
                fileName: fileName,
                performEnrichment: performEnrichment,
                useRtsApi: performEnrichment
            },
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
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
});
