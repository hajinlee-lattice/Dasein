angular
.module('mainApp.create.csvImport')
.directive('csvUploader', function ($parse) {
    return {
        restrict: 'A',
        require:'ngModel',
        link: function(scope, element, attrs, ngModel) {
            console.log('link', scope, element, attrs, ngModel);
            var model = $parse(attrs.csvUploader);
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
        controller: function ($scope, $state, $q, $element, ResourceUtility, StringUtility, csvImportService, csvImportStore) {
            var vm_form = $scope.vm,
                vm = this,
                options = {
                    process_percent: 0,
                    compress_percent: 0,
                    upload_percent: 0,
                    uploading: false,
                    uploaded: false,
                    processing: false,
                    compressing: false,
                    compressed: true,
                    csvFileName: null,
                    csvFileDisplayName: '',
                    message: 'Choose a CSV file',
                    params: vm_form.params || {}
                },
                element = this.element = $element[0];

            vm.startUpload = function() {
                if (!vm.csvFile) {
                    return false;
                }

                vm.startTime = new Date();
                vm.upload_percent = 0;
                vm.processing = true;

                vm.changeFile();
                vm.readHeaders(vm.csvFile).then(function(headers) {
                    // FIXME: emit an event, change from form controller
                    vm_form.processHeaders(headers);

                    vm.readFile(vm.csvFile)
                        .then(vm.gzipFile)
                        .then(vm.uploadFile);
                });
            }

            vm.changeFile = function(scope) {
                var input = element,
                    fileName = (navigator.userAgent.toLowerCase().indexOf('firefox') > -1)
                        ? input.value
                        : input.value.substring(12);

                vm.csvFileDisplayName = fileName;

                // FIXME: emit an event, change from form controller
                if (fileName) {
                    console.log(fileName, vm_form);
                    vm_form.generateModelName(fileName);
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
                    $scope.$apply();
                }, 500);

                var deferred = $q.defer(),
                    FR = new FileReader(),
                    sliced = file.slice(0, 2048); // grab first 2048 chars

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
                vm.compressing = true;

                setTimeout(function() {
                    vm.compress_percent = 25;
                }, 1);

                vm.compress_timer = setInterval(function() {
                    var currentTime = new Date(),
                        seconds = Math.floor((currentTime - vm.startTime) / 1000);

                    vm.compress_percent = (100 - (100 / seconds)) / 1.5;
                    vm.message = 'Compressing: ' + vm.getElapsedTime(vm.startTime);
                    $scope.$apply();
                }, 1000);

                var deferred = $q.defer(),
                    convertedData = new Uint8Array(file);

                // Workers must exist in external files.  vm fakes that.
                var blob = new Blob([
                    "onmessage = function(e) {" +
                    "   importScripts(e.data.url + '/lib/js/pako_deflate.min.js');" +
                    "   postMessage(pako.gzip(e.data.file, { to : 'Uint8Array' }));" +
                    "}"
                ]);

                // Obtain a blob URL reference to our fake worker 'file'.
                var blobURL = window.URL.createObjectURL(blob),
                    worker = new Worker(blobURL);

                worker.onmessage = function(result) {
                    var zipped = result.data,
                        array = new Array(zipped),
                        blob = new Blob(array);

                    vm.compress_percent = 67;
                    clearInterval(vm.compress_timer);
                    deferred.resolve(blob);
                };

                // pass the file and absolute URL to the worker
                worker.postMessage(
                    { url: document.location.origin, file: convertedData }, 
                    [ convertedData.buffer ]
                );

                return deferred.promise;
            }

            vm.uploadFile = function(file) {
                vm.uploading = true;
                vm.upload_percent = 0;

                if (!vm_form.params) {
                    vm_form.params = {};
                }

                var fileType = vm.accountLeadCheck ? 'SalesforceLead' : 'SalesforceAccount',
                    modelName = vm.modelDisplayName = vm.modelDisplayName || vm.csvFileName,
                    options = {
                        file: file, 
                        url: vm_form.params.url || '/pls/models/uploadfile/unnamed',
                        params: {
                            displayName: vm.csvFileName,
                            schema: vm_form.params.schema || fileType,
                            modelId: vm_form.params.modelId || false,
                            compressed: (vm_form.params.compressed || vm_form.params.compressed === false ? vm_form.params.compressed : vm.compressed)
                        },
                        progress: vm.uploadProgress
                    };
                
                vm.cancelDeferred = cancelDeferred = $q.defer();

                csvImportService.Upload(options).then(vm.uploadResponse);
            }

            vm.uploadResponse = function(result) {
                vm.uploading = false;

                if (result.Success && result.Result) {
                    var fileName = vm.choosenFileName = result.Result.name,
                        metaData = vm.metadata = result.Result;

                    vm.upload_percent = 0;
                    vm.uploaded = true;
                    vm.message = 'Done in ' + vm.getElapsedTime(vm.startTime);
                } else {
                    vm.upload_percent = 0;
                    vm.percentage = '';
                    vm.message = 'Transfer aborted';
                    vm.uploaded = false;

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

                    var xhr = csvImportStore.Get('cancelXHR', true);

                    if (xhr) {
                        xhr.abort();
                    }
                } else {
                    var done = e.loaded / 1024,
                        total = e.total / 1024,
                        percent = vm.upload_percent = ((done / total) * 100);

                    if (vm.uploading) {
                        vm.message = 'Sending: ' + vm.getElapsedTime(vm.startTime);
                        vm.percentage = Math.round(percent);
                        $scope.$apply();
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

            vm.cancel = function() {
                vm.processing = false;
                vm.compressing = false;
                vm.uploading = false;
                vm.uploaded = false;
                vm.percentage = '';
                vm.process_percent = 0;
                vm.compress_percent = 0;
                vm.upload_percent = 0;
                vm.message = '';
                //vm.csvFileName = null;
                vm.csvFileDisplayName = '';
                vm.choosenFileName = '';
            }

            angular.extend(vm, options);
        }
    };
})
.directive('csvUploaderContainer', function ($parse) {
    return {
        restrict: 'A',
        templateUrl: 'app/create/directives/CSVUploaderTemplate.html',
        controllerAs: 'vm_uploader_container',
        controller: function ($scope, $state, $q, $element, ResourceUtility, StringUtility, csvImportService, csvImportStore) {
            var vm_form = $scope.vm,
                vm = this,
                options = {

                },
                element = this.element = $element[0];

            angular.extend(vm, options);
        }
    };
});