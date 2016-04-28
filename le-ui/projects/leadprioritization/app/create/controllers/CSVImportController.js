angular.module('mainApp.create.csvImport', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.utilities.NavUtility',
    'mainApp.appCommon.utilities.StringUtility',
    '720kb.tooltips'
])
.service('csvImportStore', function() {
    this.files = {};
    this.cancelXHR = null;

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
})
.service('csvImportService', function($q, $http, ModelService, ResourceUtility, BrowserStorageUtility, csvImportStore, ServiceErrorUtility) {
    this.Upload = function(options) {
        var deferred = $q.defer(),
            formData = new FormData(),
            params = options.params;
        
        if (params.schema) {
            formData.append('schema', params.schema);
        }
        
        if (params.modelId) {
            formData.append('modelId', params.modelId);
        }
        
        if (params.displayName) {
            var name = params.displayName.replace('C:\\fakepath\\','');
            formData.append('displayName', name);
        }

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
        
        xhr.setRequestHeader("ServiceErrorMethod", (options.ServiceErrorMethod || 'modal|home.models'));

        if (BrowserStorageUtility.getTokenDocument()) {
            xhr.setRequestHeader("Authorization", BrowserStorageUtility.getTokenDocument());
        }

        xhr.setRequestHeader("Content-Encoding", "gzip");

        if (params.compressed) {
            var FR = new FileReader();

            FR.onload = function(){
                console.log('filereader onload');
                var convertedData = new Uint8Array(FR.result);

                // Zipping Uint8Array to Uint8Array
                var zippedResult = pako.gzip(convertedData, { to : "string" });

                // Need to convert back Uint8Array to ArrayBuffer for blob
                var convertedZipped = zippedResult.buffer;

                var arrayBlob = new Array(1);
                arrayBlob[0] = convertedZipped;

                // Creating a blob file with array of ArrayBuffer
                var oMyBlob = new Blob(arrayBlob); // the blob (we need to set file.type if not it defaults to application/octet-stream since it's a gzip, up to you)
                formData.append('file', oMyBlob, { type: 'application/zip' }); // we need to gzip the data
                xhr.send(formData);
                console.log('filereader onload send', formData, name || params.displayName);
            };

            FR.readAsArrayBuffer(options.file);

            formData.append('compressed', params.compressed);
        } else {
            formData.append('file', options.file);
            xhr.send(formData);
        }

        csvImportStore.Set('cancelXHR', xhr, true);

        return deferred.promise;
    };

    this.GetUnknownColumns = function(csvFile) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/models/fileuploads/' + csvFile.name + '/metadata/unknown',
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            console.log('VALIDATION SUCCESS', status, data);
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
            console.log('VALIDATION ERROR', status, data);
            var result = {
                Success: false,
                ResultErrors: data.errorMsg
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.SetUnknownColumns = function(csvMetaData, csvUnknownColumns) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/models/fileuploads/' + csvMetaData.name + '/metadata/unknown',
            data: csvUnknownColumns,
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            console.log('csvUnknownColumns POST SUCCESS', status, data);
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
            console.log('csvUnknownColumns POST ERROR', status, data);
            var result = {
                Success: false,
                ResultErrors: data.errorMsg
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.StartModeling = function(csvMetaData) {
        var deferred = $q.defer();
        console.log('StartModeling', csvMetaData)
        $http({
            method: 'POST',
            url: '/pls/models/' + csvMetaData.modelName,
            data: {
                'description': 'Self-service Model',
                'filename': csvMetaData.name,
                'name': csvMetaData.modelName,
                'displayName': csvMetaData.displayName
            },
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            console.log('MODELING POST SUCCESS', status, data);
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
            console.log('MODELING POST ERROR', status, data);
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
        console.log('TestingSet', modelId, fileName)
        $http({
            method: 'POST',
            url: '/pls/scores/' + modelId,
            params: {
                fileName: fileName
            },
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            console.log('TestingSet POST SUCCESS', status, data);
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
            console.log('TestingSet POST ERROR', status, data);
            var result = {
                Success: false,
                ResultErrors: data.errorMsg
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };
})
.directive('csvUploader', ['$parse', function ($parse) {
    return {
        restrict: 'A',
        require:'ngModel',
        link: function(scope, element, attrs, ngModel) {
            var model = $parse(attrs.csvUploader);
            var modelSetter = model.assign;

            element.bind('change', function(){
                scope.$apply(function(){
                    modelSetter(scope, element[0].files[0]);
                    ngModel.$setViewValue(element.val());
                    ngModel.$render();
                });
            });
        }
    };
}])
.controller('csvImportController', [
    '$scope', '$rootScope', 'ModelService', 'ResourceUtility', 'StringUtility', 'csvImportService', 'csvImportStore', '$state', '$q',
    function($scope, $rootScope, ModelService, ResourceUtility, StringUtility, csvImportService, csvImportStore, $state, $q) {
        $scope.showImportError = false;
        $scope.importErrorMsg = "";
        $scope.importing = false;
        $scope.showImportSuccess = false;
        $scope.ResourceUtility = ResourceUtility;
        $scope.accountLeadCheck = true;

        $scope.uploadFile = function() {
            $scope.showImportError = false;
            $scope.importErrorMsg = "";
            $scope.importing = true;

            var fileType = $scope.accountLeadCheck ? 'SalesforceLead' : 'SalesforceAccount',
                startTime = new Date();
            
            this.cancelDeferred = cancelDeferred = $q.defer();

            csvImportService.Upload({
                file: $scope.csvFile, 
                url: '/pls/models/fileuploads/unnamed',
                params: {
                    schema: fileType,
                    displayName: $scope.csvFileName
                },
                progress: function(e) {
                    if (e.total / 1024 > 486000) {
                        xhr.abort();
                        $('div.loader').css({'display':'none'});

                        html = 'ERROR: Over file size limit.  File must be below 486MB';
                    } else {
                        var done = e.loaded / 1024,
                            total = e.total / 1024,
                            percent = Math.round(done / total * 100),
                            currentTime = new Date(),
                            seconds = Math.floor((currentTime - startTime) / 1000),
                            minutes = Math.floor(seconds / 60),
                            hours = Math.floor(minutes / 60),
                            speed = done / seconds,
                            seconds = seconds % 60,
                            minutes = minutes % 60,
                            hours = hours % 24,
                            seconds = (seconds < 10 ? '0' + seconds : seconds),
                            minutes = (minutes < 10 ? '0' + minutes : minutes),
                            r = Math.round;

                        if (percent < 100) {
                            var html =  '<div style="display:inline-block;position:relative;width:164px;height:.9em;border:1px solid #aaa;padding:2px;vertical-align:top;">'+
                                        '<div style="width:'+percent+'%;height:100%;background:lightgreen;"></div></div>';
                        } else {
                            var html =  'Processing...';
                        }
                    }

                    $('#file_progress').html(html);
                }
            }).then(function(result) {
                if (result.Success && result.Result) {
                    var fileName = result.Result.name,
                        metaData = result.Result,
                        displayName = $scope.modelDisplayName,
                        modelName = StringUtility.SubstituteAllSpecialCharsWithDashes($scope.modelDisplayName);

                    metaData.modelName = modelName;
                    metaData.displayName = displayName;

                    csvImportStore.Set(fileName, metaData);

                    $state.go('home.models.import.columns', { csvFileName: fileName })
                } else {
                    console.log('# Upload Error', result);
                    $('div.loader').css({'display':'none'});

                    var errorCode = result.errorCode || 'LEDP_ERR';
                    var errorMsg  = result.errorMsg || result.ResultErrors || 'Unknown error while uploading file.';

                    html = '<span style="display:block;margin:4em auto 0.5em;max-width:27em;">' + errorMsg + '</span><span style="margin-bottom:3em;display:block;font-size:8pt;color:#666;">' + errorCode + '</span>';
                    $('#file_progress').html(html);
                }
            });

            $('#mainSummaryView .summary>h1').html('Uploading File');
            $('#mainSummaryView .summary').append('<p>Please wait while the CSV file is being uploaded.</p>');

            ShowSpinner('<div><h6 id="file_progress"></h6></div><br><button type="button" id="fileUploadCancelBtn" class="button default-button"><span style="color:black">Cancel Upload</span></button>');

            $('#fileUploadCancelBtn').on('click', $scope.cancelClicked.bind(this));
        };

        $scope.cancelClicked = function() {
            console.log('# Upload Cancelled');
            csvImportStore.Get('cancelXHR', true).abort();
            $state.go('home.models');
        };
    }
]);