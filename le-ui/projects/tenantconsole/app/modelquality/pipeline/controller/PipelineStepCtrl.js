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
        vm.error = false;

        vm.message = '';
        vm.message += !vm.stepName ? 'Step name required. ' : '';
        vm.message += !vm.pythonFile ? 'Python file required. ' : '';
        vm.message += !vm.metadataFile ? 'Metadata file required. ' : '';

        if (vm.message.length) {
            vm.error = true;
            return;
        }

        vm.loading = true;

        var promises = {
            python: ModelQualityService.UploadPythonFile(vm.stepName, vm.pythonFile.name, vm.pythonFile),
            metadata: ModelQualityService.UploadMetadataFile(vm.stepName, vm.metadataFile.name, vm.metadataFile)
        };

        $q.all(promises).then(function (results) {
            // because $q.all only catches first error, we want to catch all errors, ModelQualityService.UploadStepFile will resolve errors
            var errorMsg = _.chain(results)
                .map(function (value, key) {
                    return value.errMsg ? value.errMsg.errorCode + ': ' + value.errMsg.errorMsg + ' ' : '';
                }).uniq()
                .filter(function (value) {
                    return !!value;
                }).value().join(',');

            if (errorMsg) {
                vm.error = true;
                vm.message = errorMsg;
            } else {
                var pythonPath = results.python.resultObj.path;
                var metadataPath = results.metadata.resultObj.path;
                var dir = vm.validateAndGetPath(vm.stepName, pythonPath, metadataPath);

                if (dir) {
                    $scope.vm_analyticPipeline.addStep({
                        Name: vm.stepName,
                        pipeline_step_dir: dir,
                        isNewStep: true,
                    });

                    vm.clearForm();
                    $scope.vm_analyticPipeline.isCreatingStep = false;
                } else {
                    vm.error = true;
                    vm.message = 'Dir path error: expected same directory path but got' + pythonPath + ' and ' + metadataPath;
                }
            }

        }).finally(function () {
            vm.loading = false;
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

});
