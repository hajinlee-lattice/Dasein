angular.module("app.modelquality.controller.ModelQualityCreatePipelineStepCtrl", [
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
.controller('ModelQualityCreatePipelineStepCtrl', function ($scope, $state, $q, ModelQualityService) {

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
        pythonSuccess: null,
        pythonError: null,
        metadataSuccess: null,
        metadataError: null,
        loading: false
    });

    vm.createStep = function () {
        vm.clearMessage();

        var errorMsg = '';
        errorMsg += !vm.stepName ? 'Step name required. ' : '';
        errorMsg += !vm.pythonFile ? 'Python file required. ' : '';
        errorMsg += !vm.metadataFile ? 'Metadata file required. ' : '';

        if (errorMsg) {
            vm.setErrorMsg(errorMsg);
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
                vm.pythonError = results.python.errMsg ? results.python.errMsg.errMsg : null;
                vm.pythonSuccess = results.python.success ? 'Success' : null;
            }

            if (results.metadata) {
                vm.metadataError = results.metadata.errMsg ? results.metadata.errMsg.errMsg : null;
                vm.metadataSuccess = results.metadata.success ? 'Success' : null;
            }

            var errorMsg = _.reduce(results, function (result, value, key) {
                return result += value.errMsg ? value.errMsg.errorCode + ': ' + value.errMsg.errorMsg + ' ' : '';
            }, '');

            if (errorMsg) {
                vm.setErrorMsg(errorMsg);
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
                    vm.setErrorMsg('Dir path error: expected same directory path but got' + pythonPath + ' and ' + metadataPath);
                }
            }

        }).catch(function (error) {
            if (error && error.errMsg) {
                vm.setErrorMsg(error.errMsg);
            } else {
                vm.setErrorMsg('Unexpected error has occured. Please try again.');
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

    vm.setErrorMsg = function (msg) {
        vm.errorMsg = msg;
    };

    vm.clearForm = function () {
        vm.stepName = '';
        vm.pythonFile = null;
        vm.metadataFile = null;
    };

    vm.clearMessage = function () {
        vm.pythonError = null;
        vm.pythonSuccess = null;
        vm.metadataError = null;
        vm.metadataSuccess = null;
        vm.errorMsg = null;
    };
});
