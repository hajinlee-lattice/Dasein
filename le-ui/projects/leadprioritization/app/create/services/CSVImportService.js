angular
.module('mainApp.create.csvImport')
.service('csvImportStore', function($q) {
    var csvImportStore = this;
    this.files = {};
    this.FieldDocuments = {};
    this.CurrentFieldMapping = null;

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
.service('csvImportService', function($q, $http, ResourceUtility, BrowserStorageUtility, csvImportStore, ServiceErrorUtility) {
    this.Upload = function(options) {
        var deferred = $q.defer(),
            formData = new FormData(),
            params = options.params,
            whitelist = ['schema','modelId','description','compressed'];
        
        whitelist.forEach(function(key, value) {
            if (params[key]) {
                formData.append(key, params[key]);
            }
        })
        
        if (params.displayName) {
            var name = params.displayName.replace('C:\\fakepath\\','');
            formData.append('displayName', name);
        }

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
        xhr.setRequestHeader("Content-Encoding", "gzip");

        csvImportStore.Set('cancelXHR', xhr, true);

        formData.append('file', options.file);

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

    this.GetFieldDocument = function(csvFileName) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/models/uploadfile/' + csvFileName + '/fieldmappings',
            headers: { 'Content-Type': 'application/json' }
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

    this.SaveFieldDocuments = function(csvFileName, schemaInterpretation, fieldMappings, ignoredFields) {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'POST',
            url: '/pls/models/uploadfile/fieldmappings',
            headers: { 'Content-Type': 'application/json' },
            params: { 'displayName': csvFileName },
            data: {
                'schemaInterpretation': schemaInterpretation,
                'fieldMappings': fieldMappings,
                'ignoredFields': ignoredFields
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

    this.StartModeling = function(csvMetaData) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/models/' + csvMetaData.modelName,
            data: {
                'description': 'Self-service Model', // csvMetaData.description,
                'filename': csvMetaData.name,
                'name': csvMetaData.modelName,
                'displayName': csvMetaData.displayName
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

    this.StartTestingSet = function(modelId, fileName) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/scores/' + modelId,
            params: {
                fileName: fileName
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