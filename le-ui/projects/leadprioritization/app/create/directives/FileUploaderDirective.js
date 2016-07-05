angular
.module('lp.create.import')
.directive('fileUploaderContainer', function ($parse) {
    return {
        restrict: 'A',
        scope: {
            params:'=',
            label:'@',
            inputName:'@',
            inputDisabled:'=',
            infoTemplate:'=',
            defaultMessage:'=',
            fileAccept:'@',
            fileSelect:'&',
            fileLoad:'&',
            fileDone:'&',
            fileCancel:'&'
        },
        templateUrl: 'app/create/directives/CSVUploaderTemplate.html',
        controllerAs: 'vm_uploader_container',
        controller: function ($scope) {
            angular.extend(this, $scope);
        }
    };
})
.directive('fileUploader', function ($parse) {
    return {
        restrict: 'A',
        require:'ngModel',
        link: function(scope, element, attrs, ngModel) {
            var model = $parse(attrs.fileUploader);
            var modelSetter = model.assign;

            element.bind('change', function(){
                scope.$apply(function(){
                    modelSetter(scope, element[0].files[0]);
                    ngModel.$setViewValue(element.val());
                    ngModel.$render();

                    scope.vm_uploader.startUpload();
                });
            });
        },
        controllerAs: 'vm_uploader',
        controller: function ($scope, $state, $q, $element, $timeout, ResourceUtility, StringUtility, ImportService, ImportStore, ServiceErrorUtility) {
            var vm = this,
                options = {
                    process_percent: 0,
                    compress_percent: 0,
                    upload_percent: 0,
                    uploading: false,
                    uploaded: false,
                    processing: false,
                    compressing: false,
                    compressed: true,
                    selectedFileDisplayName: '',
                    message: $scope.defaultMessage || 'Example: us-enterprise-model.csv'
                },
                element = this.element = $element[0];

            vm.init = function() {
                console.log('init', vm, this);
                vm.params.scope = vm;
            }

            vm.startUpload = function() {
                if (!vm.selectedFile) {
                    return false;
                }

                ServiceErrorUtility.hideBanner();

                vm.startTime = new Date();
                vm.upload_percent = 0;
                vm.processing = true;

                vm.changeFile();
                vm.readHeaders(vm.selectedFile).then(function(headers) {
                    if (typeof vm.fileLoad == 'function') {
                        vm.params = vm.fileLoad({headers:headers}) || vm.params;
                    }

                    vm.readFile(vm.selectedFile)
                        .then(vm.gzipFile)
                        .then(vm.uploadFile);
                });
            }

            // grab filename from file path
            vm.getFileName = function(s) { 
              return (typeof s==='string' && (s=s.match(/[^\\\/]+$/)) && s[0]) || '';
            } 

            vm.changeFile = function(scope) {
                var input = element,
                    fileName = vm.getFileName(input.value);

                vm.selectedFileDisplayName = fileName;

                if (fileName && typeof vm.fileSelect == 'function') {
                    vm.fileSelect({fileName:fileName});
                }
            }

            vm.readHeaders = function(file) {
                vm.message = 'Processing: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                vm.process_percent = 0;
                vm.compress_percent = 0;

                setTimeout(function() {
                    vm.process_percent = 10;
                }, 1);

                vm.process_timer = setInterval(function() {
                    var currentTime = new Date(),
                        seconds = Math.floor((currentTime - vm.startTime) / 1000);

                    vm.process_percent = 100 - (100 / seconds);
                    vm.message = 'Processing: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                    $scope.$digest();
                }, 500);

                var deferred = $q.defer(),
                    FR = new FileReader(),
                    sliced = file.slice(0, 1024); // grab first 1024 chars

                FR.onload = function(e) {
                    var lines = e.target.result.split(/[\r\n]+/g);
                    deferred.resolve(lines[0]);
                };

                FR.readAsText(sliced);

                return deferred.promise;
            }

            vm.readFile = function(file) {
                var deferred = $q.defer(),
                    FR = new FileReader();

                FR.onprogress = function(e) {
                    vm.message = 'Processing: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');

                    var percent = Math.round(((e.loaded / e.total) * 100) / 3);

                    if (percent != vm.process_percent && percent > vm.process_percent) {
                        vm.process_percent = percent;
                    }
                };

                FR.onload = function(e) {
                    vm.process_percent = 33;
                    deferred.resolve(FR.result);
                };

                clearInterval(vm.process_timer);
                FR.readAsArrayBuffer(file);

                return deferred.promise;
            }

            vm.gzipFile = function(file) {
                vm.message = 'Compressing: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                vm.compressing = true;

                var deferred = $q.defer(),
                    fnComplete = function(file) {
                        vm.compress_percent = 67;
                        clearInterval(vm.compress_timer);
                        deferred.resolve(file);
                    };

                if (!vm.isCompressed()) {
                    fnComplete(vm.selectedFile);
                    return deferred.promise;
                }

                setTimeout(function() {
                    vm.compress_percent = 25;
                }, 1);

                vm.compress_timer = setInterval(function() {
                    var currentTime = new Date(),
                        seconds = Math.floor((currentTime - vm.startTime) / 1000);

                    vm.compress_percent = (100 - (100 / seconds)) / 1.5;
                    vm.message = 'Compressing: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                    $scope.$digest();
                }, 1000);

                try {
                    var convertedData = new Uint8Array(file),
                        // Workers must exist in external files.  vm fakes that
                        blob = new Blob([
                            "onmessage = function(e) {" +
                            "   importScripts(e.data.url + '/lib/js/pako_deflate.min.js');" +
                            "   postMessage(pako.gzip(e.data.file, { to : 'Uint8Array' }));" +
                            "}"
                        ]),
                        // Obtain a blob URL reference to our fake worker 'file'
                        blobURL = window.URL.createObjectURL(blob),
                        worker = new Worker(blobURL);

                    worker.onmessage = function(result) {
                        var zipped = result.data,
                            array = new Array(zipped),
                            blob = new Blob(array);

                        fnComplete(blob);
                    };

                    // pass the file and absolute URL to the worker
                    worker.postMessage(
                        { url: document.location.origin, file: convertedData }, 
                        [ convertedData.buffer ]
                    );
                } catch(e) {
                    // fallback for IE and other browsers that dont support webworker method
                    vm.message = 'Compressing the file.  This might take awhile...';

                    setTimeout(function() {
                        try {
                            var convertedData = new Uint8Array(file),
                                zipped = pako.gzip(convertedData, { to : 'Uint8Array' }),
                                array = new Array(zipped),
                                blob = new Blob(array);

                            fnComplete(blob);
                        } catch(err) {
                            // compression error, turn it off & send uncompressed
                            if (vm.params) {
                                vm.params.compressed = false;
                            } else {
                                vm.compressed = false;
                            }

                            fnComplete(vm.selectedFile);
                        }
                    }, 1);
                }

                return deferred.promise;
            }

            vm.uploadFile = function(file) {
                vm.uploading = true;
                vm.upload_percent = 0;

                if (!vm.params) {
                    vm.params = {};
                }

                var fileType = vm.accountLeadCheck ? vm.accountLeadCheck : 'SalesforceLead',
                    modelName = vm.modelDisplayName = vm.modelDisplayName || vm.selectedFileName,
                    options = {
                        file: file, 
                        url: vm.params.url || '/pls/models/uploadfile/unnamed',
                        params: {
                            schema: vm.params.schema || fileType,
                            modelId: vm.params.modelId || false,
                            metadataFile: vm.params.metadataFile || null,
                            compressed: vm.isCompressed(),
                            displayName: vm.selectedFileDisplayName
                        },
                        progress: vm.uploadProgress
                    };
                
                vm.cancelDeferred = cancelDeferred = $q.defer();

                ImportService.Upload(options).then(vm.uploadResponse);
            }

            vm.isCompressed = function() {
                if (!vm.params) {
                    vm.params = {};
                }

                return (vm.params.compressed || vm.params.compressed === false ? vm.params.compressed : vm.compressed);
            }

            vm.uploadResponse = function(result) {
                if (typeof vm.fileDone == 'function') {
                    vm.fileDone({ result: result });
                }

                vm.uploading = false;

                if (result.Success && result.Result) {
                    var fileName = vm.choosenFileName = result.Result.name,
                        metaData = vm.metadata = result.Result;

                    vm.upload_percent = 0;
                    vm.uploaded = true;
                    vm.message = 'Done in ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                } else {
                    vm.cancel(true);
                    vm.message = 'Transfer aborted';

                    setTimeout(function() {
                        vm.message = 'Choose a CSV file';
                    }, 1500);

                    var errorCode = result.errorCode || 'LEDP_ERR';
                    var errorMsg  = result.errorMsg || result.ResultErrors || 'Unknown error while uploading file.';
                }
            }

            vm.uploadProgress = function(e) {
                if (e.total / 1024 > 486000) {
                    vm.message = 'ERROR: Over 486MB file size limit.';

                    var xhr = ImportStore.Get('cancelXHR', true);

                    if (xhr) {
                        xhr.abort();
                    }
                } else {
                    var done = e.loaded / 1024,
                        total = e.total / 1024,
                        percent = vm.upload_percent = ((done / total) * 100);

                    if (vm.uploading) {
                        vm.message = 'Sending: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                        vm.percentage = Math.round(percent);
                        $scope.$digest();
                    }
                }
            }

            vm.getElapsedTime = function(startTime) {
                var format = function(num, type) {
                        if (num > 0) {
                            return num + ' ' + (num == 1 ? type : type+'s') + ' ';
                        } else {
                            return '';
                        }
                    },
                    currentTime = new Date(),
                    seconds = Math.floor((currentTime - startTime) / 1000),
                    minutes = Math.floor(seconds / 60),
                    hours = Math.floor(minutes / 60),
                    seconds = seconds % 60,
                    minutes = minutes % 60,
                    hours = hours % 24,
                    timestamp = format(hours, 'hour') + format(minutes, 'minute') + (seconds && (minutes || hours) ? ' and ' : '') + format(seconds, 'second');

                return timestamp;
            }

            vm.cancel = function(IGNORE_FILENAME) {
                vm.processing = false;
                vm.compressing = false;
                vm.uploading = false;
                vm.uploaded = false;
                vm.percentage = '';
                vm.process_percent = 0;
                vm.compress_percent = 0;
                vm.upload_percent = 0;
                vm.message = '';

                if (!IGNORE_FILENAME) {
                    //vm.selectedFileName = null;
                    vm.selectedFileDisplayName = '';
                    vm.choosenFileName = '';
                }

                if (typeof vm.fileCancel == 'function') {
                    vm.fileCancel();
                }
            }

            angular.extend(vm, $scope, options);
            vm.init();
        }
    };
});