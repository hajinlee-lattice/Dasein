angular.module("app.modelquality.controller.PipelineStepCtrl", [
])
.directive('fileModel', ['$parse', function ($parse) {
    // TODO: file uploader directive like le-ui
    return {
        restrict: 'A',
        link: function(scope, element, attrs) {
            var model = $parse(attrs.fileModel);
            var modelSetter = model.assign;

            element.bind('change', function(){
                scope.$apply(function(){
                    modelSetter(scope, element[0].files[0]);
                });
            });
        }
    };
}])
.controller('PipelineStepCtrl', function ($scope, $state, $q, ModelQualityService) {

    var vm = this;
    angular.extend(vm, {
        labels: {
            STEP_NAME: 'Step Name',
            PYTHON: 'Python File',
            METADATA: 'Metadata File',
            CREATE: 'Add Step',
            CANCEL: 'Cancel',
            LOAD_SAVING: 'Saving Step...'
        },
        errorMsg: null,
        pythonError: null,
        metadataError: null,
        loading: false,
        error: false,
        message: null
    });

    vm.createStep = function () {
        vm.clearMessage();

        vm.message = '';
        vm.message += !vm.stepName ? 'Step name required. ' : '';
        vm.message += !vm.pythonFile ? 'Python file required. ' : '';
        vm.message += !vm.metadataFile ? 'Metadata file required. ' : '';

        if (vm.message.length) {
            vm.error = true;
            return;
        }

        vm.loading = true;

        var promises = {};
        if (vm.pythonFile) {
            promises.python = ModelQualityService.UploadPythonFile(vm.stepName, vm.pythonFile.name, vm.pythonFile);
        }

        if (vm.metadataFile) {
            promises.metadata = ModelQualityService.UploadMetadataFile(vm.stepName, vm.metadataFile.name, vm.metadataFile);
        }

        $q.all(promises).then(function (results) {
            // because $q.all only catches first error, we want to catch all errors
            // ModelQualityService.UploadStepFile will resolve errors
            if (results.python) {
                vm.message += results.python.errMsg ? results.python.errMsg.errMsg : '';
            }

            if (results.metadata) {
                vm.message += results.metadata.errMsg ? results.metadata.errMsg.errMsg : '';
            }

            var errorMsg = _.reduce(results, function (result, value, key) {
                return result += value.errMsg ? value.errMsg.errorCode + ': ' + value.errMsg.errorMsg + ' ' : '';
            }, '');

            if (errorMsg) {
                vm.error = true;
                vm.message = errorMsg;
            } else {
                var pythonPath = results.python.resultObj.path;
                var metadataPath = results.metadata.resultObj.path;
                var dir = vm.validateAndGetPath(vm.stepName, pythonPath, metadataPath);

                if (dir) {
                    $scope.vm_createPipeline.addStep({
                        Name: vm.stepName,
                        pipeline_step_dir: dir,
                        isNewStep: true,
                    });

                    vm.clearForm();
                    $scope.vm_createPipeline.isCreatingStep = false;
                } else {
                    vm.error = true;
                    vm.message = 'Dir path error: expected same directory path but got' + pythonPath + ' and ' + metadataPath;
                }
            }

        }).catch(function (error) {
            vm.error = true;
            if (error && error.errMsg) {
                vm.message = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
            } else {
                vm.message = 'Unexpected error has occured. Please try again.';
            }
        }).finally(function () {
            vm.loading = false;
            vm.clearMessage();
        });
    };

    vm.validateAndGetPath = function (stepName, pythonPath, metadataPath) {
        pythonPath = pythonPath.substring(0, pythonPath.lastIndexOf('/'));
        metadataPath = metadataPath.substring(0, metadataPath.lastIndexOf('/'));

        if (pythonPath === metadataPath) {
            return pythonPath;
        } else {
            return null;
        }
    };

    vm.clearForm = function () {
        vm.stepName = '';
        vm.pythonFile = null;
        vm.metadataFile = null;
    };

    vm.clearMessage = function () {
        vm.error = false;
        vm.message = null;
    };
});
