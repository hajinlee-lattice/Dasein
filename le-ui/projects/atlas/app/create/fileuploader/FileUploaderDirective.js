angular
    .module('lp.create.import')
    .directive('fileUploaderContainer', function ($parse) {
        return {
            restrict: 'A',
            scope: {
                params: '=',
                label: '@',
                inputName: '@',
                inputDisabled: '=',
                infoTemplate: '=',
                defaultMessage: '=',
                defaultMessageFn: '&',
                hasSthreeFiles: '@',
                fileRequired: '@',
                fileAccept: '@',
                fileSelect: '&',
                fileLoad: '&',
                fileDone: '&',
                fileCancel: '&',
                fileValidation: '&',
                readOnly: '@?',
                tooltipConfig: '@'
            },
            templateUrl: 'app/create/fileuploader/FileUploaderTemplate.html',
            controllerAs: 'vm_uploader_container',
            controller: function ($scope, $state) {
                angular.extend(this, $scope);

                this.getTooltipConfig = function (key) {
                    return this.tooltipConfig != undefined ? JSON.parse(this.tooltipConfig)[key] : '';
                }

                this.getS3Files = function () {
                    $state.go('home.import.s3', {}, { reload: true } );
                }

            }
        };
    })
    .directive('fileUploader', function ($parse) {
        return {
            restrict: 'A',
            require: 'ngModel',
            link: function (scope, element, attrs, ngModel) {
                var model = $parse(attrs.fileUploader);
                var modelSetter = model.assign;
                element.bind('change', function () {
                    scope.$apply(function () {
                        modelSetter(scope, element[0].files[0]);
                        ngModel.$setViewValue(element.val());
                        ngModel.$render();

                        scope.vm_uploader.startUpload();
                    });
                });
            },
            controllerAs: 'vm_uploader',
            controller: function ($scope, $state, $q, $element, ImportService, ImportStore, ImportWizardStore, ServiceErrorUtility, Banner, Notice) {
                var vm = this,
                    options = {
                        compress_percent: 0,
                        upload_percent: 0,
                        uploading: false,
                        uploaded: false,
                        compressing: false,
                        compressed: true,
                        selectedFileDisplayName: '',
                        defaultMessage: $scope.defaultMessage || 'Example: us-enterprise-model.csv',
                        message: ''
                    },
                    element = this.element = $element[0],
                    GB = 1073741824,
                    GBLimit = 2;

                vm.file = {};

                vm.init = function () {
                    vm.params.scope = vm;
                    vm.fileRequired = typeof vm.fileRequired == "undefined"
                        ? true
                        : vm.fileRequired;

                }

                vm.startUpload = function () {
                    if (!vm.selectedFile) {
                        return false;
                    }

                    if (!vm.params.noSizeLimit && vm.selectedFile.size > (GB * GBLimit)) {
                        Banner.error({ title: 'Error', message: 'Your file is too large.  Please try again with a file that is smaller than ' + GBLimit + 'GB.' });
                        return;
                    } else if (!vm.matchesFileExtension(vm.selectedFileName)) {
                        var fileExt = vm.fileAccept ? vm.fileAccept.split(',')[0].split('.').pop().toUpperCase() : 'CSV'; //get first file extension in file-accept or use CSV as default;
                        Banner.error({ title: 'Error', message: 'Invalid file format. Please try uploading the file in ' + fileExt + ' format.' });
                        return;
                    }

                    // ServiceErrorUtility.hideBanner();

                    vm.startTime = new Date();
                    vm.upload_percent = 0;

                    vm.changeFile();
                    vm.readHeaders(vm.selectedFile).then(function (headers) {
                        if (typeof vm.fileLoad == 'function') {
                            vm.params = vm.fileLoad({ headers: headers }) || vm.params;
                        }
                        if (vm.params.importError) {
                            var result = { Success: false };
                            vm.uploadResponse(result);
                            return;
                        }

                        var fnFallBack = function () {
                            vm.compress_percent = 100;
                            vm.uploadFile(vm.selectedFile);
                        }

                        if (vm.isCompressed(vm.selectedFile)) {
                            try {
                                vm.processFile(vm.selectedFile)
                                    .then(vm.uploadFile);
                            } catch (e) {
                                fnFallBack();
                            }
                        } else {
                            fnFallBack();
                        }
                    });
                }

                vm.getFileName = function (s) {
                    return (typeof s === 'string' && (s = s.match(/[^\\\/]+$/)) && s[0]) || '';
                }

                vm.getFileExtension = function (fileName) {
                    return fileName.split('.').pop();
                }

                vm.matchesFileExtension = function (fileName) {
                    if (vm.fileAccept) {
                        return vm.fileAccept.includes(vm.getFileExtension(fileName));
                    } else {
                        return vm.getFileExtension(vm.selectedFileName) == 'csv';
                    }
                }

                vm.changeFile = function (scope) {
                    vm.cancel(true);
                    var input = element,
                        fileName = vm.getFileName(input.value);

                    vm.selectedFileDisplayName = fileName;

                    if (fileName && typeof vm.fileSelect == 'function') {
                        vm.fileSelect({ fileName: fileName });
                    }
                }

                vm.readHeaders = function (file) {
                    vm.message = 'Compressing: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                    vm.compress_percent = 0;

                    var deferred = $q.defer(),
                        FR = new FileReader(),
                        sliced = file.slice(0, 1024 * 8); // grab first 1024 * 8 chars

                    FR.onload = function (e) {
                        var lines = e.target.result.split(/[\r\n]+/g);
                        deferred.resolve(lines[0]);
                    };

                    FR.readAsText(sliced);

                    return deferred.promise;
                }

                vm.processFile = function (file) {
                    var deferred = $q.defer();

                    try {
                        // make 16k chunks, compress chunks, reconstitute
                        vm.processInChunks(file).then(function (result) {
                            deferred.resolve(result);
                        });
                    } catch (err) {
                        // read whole file, compress whole file
                        vm.processWhole(file).then(function (result) {
                            deferred.resolve(result);
                        });
                    }

                    return deferred.promise;
                }

                vm.processWhole = function (file) {
                    var deferred = $q.defer();

                    vm.readFile(file)
                        .then(vm.compressFileInWorker)
                        .then(function (result) {
                            deferred.resolve(result);
                        });

                    return deferred.promise;
                }

                vm.processInChunks = function (file) {
                    vm.compressing = true;

                    var deferred = $q.defer(),
                        blob = new Blob([
                            "onmessage = function(e) {" +
                            "   importScripts(e.data.url + '/lib/js/pako_deflate.min.js');" +
                            "" +
                            "   var file = e.data.file," +
                            "       FR = new FileReaderSync()," +
                            "       deflator = new pako.Deflate({ gzip: true })," +
                            "       totalSize = file.size," +
                            "       maxChunk = 16384," +
                            "       chunkSize = (totalSize < maxChunk ? totalSize : maxChunk)," +
                            "       curSize = 0," +
                            "       chunks = 0," +
                            "       chunk;" +
                            "" +
                            "   while (curSize < totalSize) { " +
                            "       if (chunks++ % 100 == 0) {" +
                            "           var percentage = ((curSize / totalSize) * 100).toFixed(2);" +
                            "           postMessage({ file: null, progress: percentage });" +
                            "       }" +
                            "" +
                            "       chunk = file.slice(curSize, curSize + chunkSize);" +
                            "       curSize += chunkSize;" +
                            "       lastChunk = (curSize >= totalSize);" +
                            "       deflator.push(FR.readAsArrayBuffer(chunk), lastChunk);" +
                            "   }" +
                            "" +
                            "   postMessage({ file: deflator.result, progress: 100 });" +
                            "}"
                        ]),
                        blobURL = window.URL.createObjectURL(blob),
                        worker = new Worker(blobURL);

                    worker.onmessage = function (result) {
                        if (result.data.progress) {
                            vm.message = 'Compressing: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                            vm.compress_percent = result.data.progress;
                            vm.percentage = Math.ceil(result.data.progress);
                            $scope.$digest();
                        }

                        if (result.data.file) {
                            var blob = new Blob([new Uint8Array(result.data.file)]);
                            deferred.resolve(blob);
                        }
                    };

                    worker.postMessage({
                        url: document.location.origin,
                        file: file
                    });

                    return deferred.promise;
                }

                vm.readFile = function (file) {
                    var deferred = $q.defer(),
                        FR = new FileReader();

                    FR.onload = function (e) {
                        deferred.resolve(FR.result);
                    };

                    FR.readAsArrayBuffer(file);

                    return deferred.promise;
                }

                vm.readFileAsText = function (file) {
                    var deferred = $q.defer(),
                        FR = new FileReader();

                    FR.onload = function (e) {
                        deferred.resolve(FR.result);
                    };

                    FR.readAsText(file);

                    return deferred.promise;
                }

                vm.compressFileInWorker = function (file) {
                    vm.message = 'Compressing: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                    vm.compressing = true;

                    var deferred = $q.defer(),
                        fnComplete = function (file) {
                            vm.compress_percent = 67;
                            clearInterval(vm.compress_timer);
                            deferred.resolve(file);
                        };

                    vm.compress_timer = setInterval(function () {
                        var currentTime = new Date(),
                            seconds = Math.floor((currentTime - vm.startTime) / 1000);

                        vm.compress_percent = 100 - (100 / seconds);
                        vm.message = 'Compressing: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                        $scope.$digest();
                    }, 1000);

                    try {
                        var convertedData = new Uint8Array(file),
                            // Workers must exist in external files. this fakes that
                            blob = new Blob([
                                "onmessage = function(e) {" +
                                "   importScripts(e.data.url + '/lib/js/pako_deflate.min.js');" +
                                "   postMessage(pako.gzip(e.data.file, { to : 'Uint8Array' }));" +
                                "}"
                            ]),
                            // Obtain a blob URL reference to our fake worker 'file'
                            blobURL = window.URL.createObjectURL(blob),
                            worker = new Worker(blobURL);

                        worker.onmessage = function (result) {
                            var zipped = result.data,
                                array = new Array(zipped),
                                blob = new Blob(array);

                            fnComplete(blob);
                        };

                        // pass the file and absolute URL to the worker
                        worker.postMessage(
                            { url: document.location.origin, file: convertedData },
                            [convertedData.buffer]
                        );
                    } catch (err) {
                        // Web Workers not working, try synchronous
                        vm.compressFile(file).then(function (result) {
                            deferred.resolve(result);
                        });
                    }

                    return deferred.promise;
                }

                vm.compressFile = function (file) {

                    console.log(fileType);

                    vm.compressing = true;
                    vm.message = 'Compressing the file.  This might take awhile...';

                    var deferred = $q.defer(),
                        fnComplete = function (file) {
                            vm.compress_percent = 100;
                            clearInterval(vm.compress_timer);
                            deferred.resolve(file);
                        };

                    // fallback for IE and other browsers that dont support webworker method
                    setTimeout(function () {
                        try {
                            var convertedData = new Uint8Array(file),
                                zipped = pako.gzip(convertedData, { to: 'Uint8Array' }),
                                array = new Array(zipped),
                                blob = new Blob(array);

                            fnComplete(blob);
                        } catch (err) {
                            // compression error, turn it off & send uncompressed
                            if (vm.params) {
                                vm.params.compressed = false;
                            } else {
                                vm.compressed = false;
                            }

                            fnComplete(vm.selectedFile);
                        }
                    }, 1);

                    return deferred.promise;
                }

                vm.uploadFile = function (file) {
                    vm.uploading = true;
                    vm.upload_percent = 0;
                    var cancelDeferred;
                    if (!vm.params) {
                        vm.params = {};
                    }

                    var fileType = vm.accountLeadCheck ? vm.accountLeadCheck : 'SalesforceLead',
                        modelName = vm.modelDisplayName = vm.modelDisplayName || vm.selectedFileName, options;

                    if (!vm.readOnly) {
                        if ($state.includes('home.import.entry')) {
                            var fileName = "file_" + (new Date).getTime() + ".csv";
                            ImportWizardStore.setCsvFileName(fileName);
                            options = {
                                file: file,
                                url: vm.params.url || '/pls/models/uploadfile',
                                params: {
                                    entity: 'account',
                                    fileName: fileName,
                                    modelId: vm.params.modelId || false,
                                    metadataFile: vm.params.metadataFile || null,
                                    compressed: vm.isCompressed(),
                                    displayName: vm.selectedFileDisplayName
                                },
                                progress: vm.uploadProgress
                            };
                        } else {
                            options = {
                                file: file,
                                url: vm.params.url || '/pls/models/uploadfile/unnamed',
                                params: {
                                    schema: vm.params.schema || fileType,
                                    modelId: vm.params.modelId || false,
                                    metadataFile: vm.params.metadataFile || null,
                                    compressed: vm.isCompressed(),
                                    displayName: vm.selectedFileDisplayName,
                                    operationType: vm.params.operationType || null
                                },
                                progress: vm.uploadProgress
                            };
                        }
                        vm.cancelDeferred = cancelDeferred = $q.defer();

                        console.log(options);


                        ImportService.Upload(options).then(vm.uploadResponse);
                    } else {
                        vm.readFileAsText(file).then(vm.uploadResponse);
                    }
                }

                vm.isCompressed = function (file) {
                    //console.log('file',file)
                    if (!vm.params) {
                        vm.params = {};
                    }

                    // don't bother compressing if file size is small
                    if (file && file.size < 16384) {
                        vm.params.compressed = false;
                    }

                    return (vm.params.compressed || vm.params.compressed === false ? vm.params.compressed : vm.compressed);
                }

                vm.uploadResponse = function (result) {

                    console.log(result);

                    if (typeof vm.fileDone == 'function') {
                        vm.fileDone({ result: result });
                    }

                    vm.uploading = false;

                    if (vm.readOnly && typeof vm.fileValidation == 'function') {
                        vm.fileValidation({ result: result }).then(function (response) {
                            if (response) {
                                vm.choosenFileName = vm.selectedFileDisplayName,
                                    vm.metadata = {};

                                vm.completeTransfer();
                            } else {

                                vm.abortTransfer(result);
                            }
                        });
                    } else {
                        if ((result.Success && result.Result)) {
                            var fileName = vm.choosenFileName = result.Result ? result.Result.name : vm.selectedFileDisplayName,
                                metaData = vm.metadata = result.Result || {};

                            vm.completeTransfer();
                        } else {
                            vm.abortTransfer(result);
                            // removed as per PLS-11516
                            // var msg = result.ResultErrors ? result.ResultErrors : 'The file upload was aborted';
                            // Notice.warning({message: msg});
                        }
                    }
                }

                vm.completeTransfer = function () {
                    vm.upload_percent = 0;
                    vm.uploaded = true;
                    vm.message = 'Done in ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                }

                vm.abortTransfer = function (result) {
                    vm.cancel(true, result);
                    vm.message = 'Transfer aborted';

                    setTimeout(function () {
                        vm.message = '';
                    }, 1500);
                }

                vm.uploadProgress = function (e) {
                    if (e.total / 1024 > 4194304) {
                        vm.message = 'ERROR: Over ~' + GBLimit + 'GB file size limit.';

                        var xhr = ImportStore.Get('cancelXHR', true);

                        if (xhr) {
                            xhr.abort();
                        }
                    } else {
                        var done = e.loaded / 1024,
                            total = e.total / 1024,
                            percent = vm.upload_percent = ((done / total) * 100);

                        if (vm.uploading) {
                            if (percent < 100) {
                                vm.message = 'Sending: ' + (vm.getElapsedTime(vm.startTime) || '0 seconds');
                            } else {
                                vm.message = 'Processing...';
                            }
                            vm.percentage = percent ? Math.ceil(percent) : 1;
                            $scope.$digest();
                        }
                    }
                }

                vm.getElapsedTime = function (startTime) {
                    var format = function (num, type) {
                        if (num > 0) {
                            return num + ' ' + (num == 1 ? type : type + 's') + ' ';
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

                vm.cancel = function (IGNORE_FILENAME, data) {
                    vm.compressing = false;
                    vm.uploading = false;
                    vm.uploaded = false;
                    vm.percentage = '';
                    vm.compress_percent = 0;
                    vm.upload_percent = 0;
                    vm.message = '';

                    if (!IGNORE_FILENAME) {
                        //vm.selectedFileName = null;
                        vm.selectedFileDisplayName = '';
                        vm.choosenFileName = '';
                    }

                    if (typeof vm.fileCancel == 'function') {
                        vm.fileCancel({ data: data });
                    }
                    setTimeout(function () { $scope.$apply(); }, 100);

                }

                vm.showFileDisplayName = function () {
                    return vm.compressing || vm.uploading || vm.uploaded;
                }

                vm.showFileIcon = function () {
                    return !vm.compressing && !vm.uploading && !vm.uploaded;
                }

                vm.showSpinnerIcon = function () {
                    return vm.compressing && vm.compress_percent < 100;
                }

                vm.showCancelIcon = function () {
                    return vm.uploading && vm.upload_percent > 0;
                }

                vm.showCheckIcon = function () {
                    return vm.uploaded;
                }

                vm.showCompressingBar = function () {
                    return vm.processing || vm.compressing;
                }

                vm.showUploadingBar = function () {
                    return vm.uploading || vm.uploaded;
                }

                angular.extend(vm, $scope, options);

                vm.init();
            }
        };
    });