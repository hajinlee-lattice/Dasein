angular
    .module('lp.create.import')
    .service('ImportStore', function ($q) {
        var ImportStore = this;

        this.files = {};

        this.FieldDocuments = {};
        this.CurrentFieldMapping = null;
        this.ScoreFieldDocuments = {};
        this.ScoreCurrentFieldMapping = null;

        this.advancedSettings = {
            oneLeadPerDomain: true,
            includePersonalEmailDomains: true,
            useLatticeAttributes: true,
            enableTransformations: true
        };

        this.SetAdvancedSettings = function (key, value) {
            this.advancedSettings[key] = value;
        };

        this.GetAdvancedSetting = function (key) {
            return this.advancedSettings[key];
        };

        this.ResetAdvancedSettings = function () {
            this.advancedSettings.oneLeadPerDomain = true;
            this.advancedSettings.includePersonalEmailDomains = true;
            this.advancedSettings.useLatticeAttributes = true;
            this.advancedSettings.enableTransformations = true;
        };

        this.Get = function (name, root) {
            return root ? this[name] : this.files[name];
        }

        this.Set = function (name, data, root) {
            if (root) {
                this[name] = data;
            } else {
                this.files[name] = data;
            }
        }

        this.GetFieldDocument = function (name) {
            var deferred = $q.defer();

            if (this.FieldDocuments[name]) {
                deferred.resolve(this.FieldDocuments[name]);
            }

            return deferred.promise;
        }

        this.SetFieldDocument = function (name, data) {
            this.FieldDocuments[name] = data;
        }

        this.GetScoreFieldDocument = function (name) {
            var deferred = $q.defer();

            if (this.ScoreFieldDocuments[name]) {
                deferred.resolve(this.ScoreFieldDocuments[name]);
            }

            return deferred.promise;
        }

        this.SetScoreFieldDocument = function (name, data) {
            this.ScoreFieldDocuments[name] = data;
        }
    })
    .service('ImportService', function ($q, $http, $stateParams, ResourceUtility, BrowserStorageUtility, ImportStore, ServiceErrorUtility) {
        this.Upload = function (options) {
            var deferred = $q.defer(),
                formData = new FormData(),
                params = options.params || {},
                whitelist = [
                    'schema', 'modelId', 'notesContent', 'compressed',
                    'displayName', 'file', 'metadataFile', 'entity', 'fileName', 'operationType'
                ];

            if (params.metadataFile) {
                params['metadataFile'] = options.file;
            } else if (options.file) {
                params['file'] = options.file;
            }

            if (params.displayName) {
                params['displayName'] = params.displayName.replace('C:\\fakepath\\', '');
            }

            whitelist.forEach(function (key, value) {
                if (params[key] && (params[key] != null || params[key] != undefined)) {
                    formData.append(key, params[key]);
                }
            });

            // can't use $http because it does not expose onprogress event
            var xhr = new XMLHttpRequest();

            if (options.progress) {
                (xhr.upload || xhr).addEventListener('progress', options.progress);
            }

            xhr.addEventListener('load', function (event) {
                var status = this.status;
                if (status != 200) {
                    var obj = JSON.parse(this.responseText);
                    var txt = obj.errorMsg || obj.responseText;
                    var result = {
                        Success: false,
                        ResultErrors: txt,
                        Result: null,
                        data: {
                            ...obj
                        }
                    };
                    ServiceErrorUtility.process(result);
                    deferred.resolve(result);
                } else {
                    xhr.data = JSON.parse(this.responseText);
                    ServiceErrorUtility.process(xhr);
                    deferred.resolve(xhr.data);
                }
            });

            xhr.addEventListener('error', function (event) {
                xhr.data = JSON.parse(this.responseText);
                ServiceErrorUtility.process(xhr);

                var result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('MODEL_IMPORT_CONNECTION_ERROR'),
                    Result: null
                };

                deferred.resolve(result);
            });

            xhr.addEventListener('abort', function (event) {
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

        this.GetSchemaToLatticeFields = function (csvFileName) {
            var deferred = $q.defer();
            var params = csvFileName ? { 'csvFileName': csvFileName } : { 'excludeLatticeDataAttributes': ImportStore.GetAdvancedSetting('useLatticeAttributes') ? false : true };

            $http({
                method: 'GET',
                url: csvFileName
                    ? '/pls/scores/fileuploads/headerfields'
                    : '/pls/models/uploadfile/latticeschema',
                params: params,
                headers: { 'Content-Type': 'application/json' }
            }).then(function (data) {
                deferred.resolve(data.data.Result);
            });

            return deferred.promise;
        };

        this.GetFieldDocument = function (FileName, score) {
            var deferred = $q.defer();
            var modelId = $stateParams.modelId;
            var metaData = ImportStore.Get(FileName);
            var schema = metaData.schemaInterpretation;
            var params = score ? { 'modelId': modelId, 'csvFileName': FileName } : { 'schema': schema };

            $http({
                method: 'POST',
                url: score
                    ? '/pls/scores/fileuploads/fieldmappings'
                    : '/pls/models/uploadfile/' + FileName + '/fieldmappings',
                params: params,
                headers: { 'Content-Type': 'application/json' },
                data: {
                    deduplicationType: ImportStore.GetAdvancedSetting('oneLeadPerDomain') ? 'ONELEADPERDOMAIN' : 'MULTIPLELEADSPERDOMAIN',
                    excludePublicDomains: ImportStore.GetAdvancedSetting('includePersonalEmailDomains') ? false : true,
                    excludePropDataColumns: ImportStore.GetAdvancedSetting('useLatticeAttributes') ? false : true,
                    transformationGroup: ImportStore.GetAdvancedSetting('enableTransformations') ? null : 'none',
                }
            })
                .success(function (data, status, headers, config) {

                    console.log("GetFieldDocument:", data);

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
                    }

                    deferred.resolve(result);
                })
                .error(function (data, status, headers, config) {
                    var result = {
                        Success: false,
                        ResultErrors: data.errorMsg
                    };

                    deferred.resolve(result);
                });

            return deferred.promise;
        };

        this.SaveFieldDocuments = function (FileName, FieldDocument, score) {
            var deferred = $q.defer();
            var result;
            var modelId = $stateParams.modelId;
            var params = score ? { 'modelId': modelId, 'csvFileName': FileName } : { 'displayName': FileName };

            $http({
                method: 'POST',
                url: score
                    ? '/pls/scores/fileuploads/fieldmappings/resolve'
                    : '/pls/models/uploadfile/fieldmappings',
                headers: { 'Content-Type': 'application/json' },
                params: params,
                data: {
                    'fieldMappings': FieldDocument.fieldMappings,
                    'ignoredFields': FieldDocument.ignoredFields
                }
            })
                .success(function (data, status, headers, config) {
                    console.log("SaveFieldDocuments:", data);
                    deferred.resolve(result);
                })
                .error(function (data, status, headers, config) {
                    deferred.resolve(result);
                });

            return deferred.promise;
        };

        this.StartModeling = function (MetaData) {
            var deferred = $q.defer();
            var data = {
                notesContent: MetaData.notesContent,
                filename: MetaData.name,
                name: MetaData.modelName,
                deduplicationType: ImportStore.GetAdvancedSetting('oneLeadPerDomain') ? 'ONELEADPERDOMAIN' : 'MULTIPLELEADSPERDOMAIN',
                excludePublicDomains: ImportStore.GetAdvancedSetting('includePersonalEmailDomains') ? false : true,
                excludePropDataColumns: ImportStore.GetAdvancedSetting('useLatticeAttributes') ? false : true,
                transformationGroup: ImportStore.GetAdvancedSetting('enableTransformations') ? null : 'none',
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
                .success(function (data, status, headers, config) {
                    console.log("StartModeling:", data);
                    if (data == null) {
                        var result = {
                            Success: false,
                            ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
                            Result: null
                        };
                    } else {
                        var result = {
                            Success: true,
                            ResultErrors: data.Errors,
                            Result: data.Result
                        };
                    }

                    deferred.resolve(result);
                })
                .error(function (data, status, headers, config) {
                    var result = {
                        Success: false,
                        ResultErrors: data.errorMsg
                    };

                    deferred.resolve(result);
                });

            return deferred.promise;
        };

        this.StartPMMLModeling = function (options) {
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
                .success(function (data, status, headers, config) {

                    console.log("StartPMMLModeling:", data);
                    if (data == null) {
                        var result = {
                            Success: false,
                            ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
                            Result: null
                        };
                    } else {
                        var result = {
                            Success: true,
                            ResultErrors: data.Errors,
                            Result: data.Result
                        };
                    }

                    deferred.resolve(result);
                })
                .error(function (data, status, headers, config) {
                    var result = {
                        Success: false,
                        ResultErrors: data.errorMsg
                    };

                    deferred.resolve(result);
                });

            return deferred.promise;
        };

        this.StartTestingSet = function (modelId, fileName, performEnrichment) {
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
                .success(function (data, status, headers, config) {
                    console.log("StartTestingSet:", data);
                    if (data == null) {
                        var result = {
                            Success: false,
                            ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
                            Result: null
                        };
                    } else {
                        var result = {
                            Success: true,
                            ResultErrors: data.Errors,
                            Result: data.Result
                        };
                    }

                    deferred.resolve(result);
                })
                .error(function (data, status, headers, config) {
                    var result = {
                        Success: false,
                        ResultErrors: data.errorMsg
                    };

                    deferred.resolve(result);
                });

            return deferred.promise;
        };
    });
