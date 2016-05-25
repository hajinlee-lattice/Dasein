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
        
        if (params.description) {
            formData.append('description', params.description);
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
        

        if (BrowserStorageUtility.getTokenDocument()) {
            xhr.setRequestHeader("Authorization", BrowserStorageUtility.getTokenDocument());
        }

        xhr.setRequestHeader("ErrorDisplayMethod", (options.ErrorDisplayMethod || 'banner'));
        xhr.setRequestHeader("Content-Encoding", "gzip");

        csvImportStore.Set('cancelXHR', xhr, true);

        if (params.compressed) {
            this.gzipFile(options.file).then(function(result) {
                var zipped = result.data,
                    array = new Array(zipped),
                    blob = new Blob(array);

                // Creating a blob file with array of ArrayBuffer
                formData.append('file', blob);
                formData.append('compressed', params.compressed);

                xhr.send(formData);
            });
        } else {
            formData.append('file', options.file);
            xhr.send(formData);
        }
        
        return deferred.promise;
    };

    this.gzipFile = function(file) {
        var deferred = $q.defer(),
            FR = new FileReader();

        FR.onload = function() {
            var convertedData = new Uint8Array(FR.result);

            // Workers must exist in external files.  This fakes that.
            var blob = new Blob([
                "onmessage = function(e) {" +
                "   importScripts(e.data.url + '/lib/js/pako_deflate.min.js');" +
                "   postMessage(pako.gzip(e.data.file, { to : 'Uint8Array' }));" +
                "}"
            ]);

            // Obtain a blob URL reference to our fake worker 'file'.
            var blobURL = window.URL.createObjectURL(blob);
            var worker = new Worker(blobURL);

            worker.onmessage = function(e) {
                deferred.resolve(e);
            };

            // pass the file and absolute URL to the worker
            worker.postMessage(
                { url: document.location.origin, file: convertedData }, 
                [ convertedData.buffer ]
            );
        };

        FR.readAsArrayBuffer(file);

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
        }).success(function(data, status, headers, config) {
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
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
                'description': 'Self-service Model', // csvMetaData.description,
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
                scope.vm.cancelClicked();

                var input = this;
                
                scope.$apply(function(){
                    var fileName = (navigator.userAgent.toLowerCase().indexOf('firefox') > -1)
                        ? input.value
                        : input.value.substring(12);

                    scope.vm.csvFileDisplayName = fileName;

                    if (!scope.vm.modelDisplayName) {
                        var date = new Date(),
                            day = date.getDate(),
                            year = date.getFullYear(),
                            month = (date.getMonth() + 1),
                            seconds = date.getSeconds(),
                            minutes = date.getMinutes(),
                            hours = date.getHours(),
                            month = (month < 10 ? '0' + month : month),
                            day = (day < 10 ? '0' + day : day),
                            minutes = (minutes < 10 ? '0' + minutes : minutes),
                            hours = (hours < 10 ? '0' + hours : hours),
                            timestamp = year +''+ month +''+ day +'-'+ hours +''+ minutes,
                            displayName = fileName.replace('.csv',''),
                            displayName = displayName.substr(0, 50 - (timestamp.length));

                        if (scope.vm.modelDisplayName.indexOf(displayName) < 0) {
                            scope.vm.modelDisplayName = displayName + '_' + timestamp;
                        }

                        $('#modelDisplayName').focus();
                        
                        setTimeout(function() {
                            $('#modelDisplayName').select();
                        }, 1);
                    }

                    modelSetter(scope, element[0].files[0]);
                    ngModel.$setViewValue(element.val());
                    ngModel.$render();
                    
                    if (fileName) {
                        scope.vm.uploadFile();
                    }
                });
            });
        }
    };
}])
.controller('csvImportController', [
    '$scope', 'ModelService', 'ResourceUtility', 'StringUtility', 'csvImportService', 'csvImportStore', '$state', '$q',
    function($scope, ModelService, ResourceUtility, StringUtility, csvImportService, csvImportStore, $state, $q) {
        var vm = this;

        vm.uploading = false;
        vm.uploaded = false;
        vm.compressed = true;
        vm.showImportError = false;
        vm.showImportSuccess = false;
        vm.accountLeadCheck = true;
        vm.ResourceUtility = ResourceUtility;
        vm.csvFileDisplayName = '';
        vm.modelDisplayName = '';
        vm.importErrorMsg = '';
        vm.modelDescription = '';
        vm.message = 'Choose a CSV file';

        vm.uploadFile = function() {
            vm.showImportError = false;
            vm.importErrorMsg = "";
            vm.uploading = true;
            vm.percent = 0;
            vm.message = 'Preparing file...'

            var fileType = vm.accountLeadCheck ? 'SalesforceLead' : 'SalesforceAccount',
                modelName = vm.modelDisplayName = vm.modelDisplayName || vm.csvFileName,
                startTime = new Date();
            
            this.cancelDeferred = cancelDeferred = $q.defer();
            csvImportService.Upload({
                file: vm.csvFile, 
                url: '/pls/models/uploadfile/unnamed',
                // url: '/pls/models/fileuploads/unnamed',
                params: {
                    // schema: fileType,
                    displayName: vm.csvFileName,
                    compressed: vm.compressed
                },
                progress: function(e) {
                    if (e.total / 1024 > 486000) {
                        xhr.abort();
                        $('div.loader').css({'display':'none'});

                        $('#fileUploadCancelBtn').html('ERROR: Over file size limit.  File must be below 486MB');
                    } else {
                        var format = function(num, type) {
                                if (num > 0) {
                                    return num + ' ' + (num == 1 ? type : type+'s') + ' ';
                                } else {
                                    return '';
                                }
                            },
                            done = e.loaded / 1024,
                            total = e.total / 1024,
                            percent = vm.percent = Math.round(done / total * 100),
                            currentTime = new Date(),
                            seconds = Math.floor((currentTime - startTime) / 1000),
                            minutes = Math.floor(seconds / 60),
                            hours = Math.floor(minutes / 60),
                            speed = done / seconds,
                            seconds = seconds % 60,
                            minutes = minutes % 60,
                            hours = hours % 24,
                            r = Math.round;

                        if (vm.uploading) {
                            vm.message = format(hours, 'hour') + format(minutes, 'minute') + format(seconds, 'second');
                            vm.percentage = percent;
                            $scope.$apply();
                        }
                    }
                }
            }).then(function(result) {
                vm.uploading = false;

                if (result.Success && result.Result) {
                    var fileName = vm.choosenFileName = result.Result.name,
                        metaData = vm.metadata = result.Result;

                    vm.percent = 0;
                    vm.uploaded = true;
                    vm.message = 'Done.';

                    /*

                    */
                } else {
                    vm.percent = 0;
                    vm.percentage = '';
                    vm.message = 'Transfer aborted';

                    setTimeout(function() {
                        vm.message = 'Choose a CSV file';
                    }, 1500);

                    console.log('# Upload Aborted', result);

                    var errorCode = result.errorCode || 'LEDP_ERR';
                    var errorMsg  = result.errorMsg || result.ResultErrors || 'Unknown error while uploading file.';
                }
            });

            $('#fileUploadCancelBtn').on('click', vm.cancelClicked.bind(this));
        };

        vm.clickNext = function() {
            var fileName = vm.choosenFileName;
                metaData = vm.metadata,
                displayName = vm.modelDisplayName,
                modelName = StringUtility.SubstituteAllSpecialCharsWithDashes(displayName);

            metaData.modelName = modelName;
            metaData.displayName = displayName;
            metaData.description = vm.modelDescription;
            //console.log(fileName, metaData);
            csvImportStore.Set(fileName, metaData);

            setTimeout(function() {
                $state.go('home.models.import.columns', { csvFileName: fileName });
            }, 1);
        }

        vm.cancelClicked = function() {
            vm.uploading = false;
            vm.uploaded = false;
            vm.percentage = '';
            vm.percent = 0;
            vm.message = '';
            var xhr = csvImportStore.Get('cancelXHR', true);
            
            if (xhr) {
                xhr.abort();
            }
        };

        function expandTextarea(id) {
            document.getElementById(id).addEventListener('keyup', function() {
                this.style.overflow = 'hidden';
                this.style.height = 0;
                this.style.height = this.scrollHeight + 'px';
            }, false);
        }

        expandTextarea('modelDescription');

    }
]);