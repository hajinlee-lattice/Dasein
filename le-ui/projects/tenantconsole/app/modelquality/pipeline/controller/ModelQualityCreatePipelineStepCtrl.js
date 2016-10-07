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
        stepNameRequired: null,
        errorMsg: null,
        pythonSuccess: null,
        metadataSuccess: null,
        loading: false
    });

    vm.createStep = function () {
        if (!vm.stepName) {
            vm.stepNameRequired = 'Step name required';
            return;
        }

        vm.stepNameRequired = null;
        vm.errorMsg = null;
        vm.pythonSuccess = null;
        vm.metadataSuccess = null;
        vm.loading = true;

        var promises = {};
        if (vm.pythonFile) {
            promises.python = ModelQualityService.UploadPythonFile(vm.stepName, vm.pythonFile.name, vm.pythonFile);
        }

        if (vm.metadataFile) {
            promises.metadata = ModelQualityService.UploadMetadataFile(vm.stepName, vm.metadataFile.name, vm.metadataFile);
        }

        $q.all(promises).then(function (results) {

            if (results.python) {
                vm.pythonError = results.python.errMsg ? results.python.errMsg.errMsg : null;
                vm.pythonSuccess = results.python.success ? 'Success' : null;
            }

            if (results.metadata) {
                vm.metadataError = results.metadata.errMsg ? results.metadata.errMsg.errMsg : null;
                vm.metadataSuccess = results.metadata.success ? 'Success' : null;
            }

            var hasError = Object.keys(results).reduce(function (acc, cur) {
                return acc += results[cur].errMsg ? (error.errMsg.errorCode + ': ' + error.errMsg.errorMsg) : '';
            }, '');


            if (hasError) {
                vm.errorMsg = hasError;
            } else {

                $scope.vm_createPipeline.addStep({ Name: vm.stepName });

                vm.stepName = '';
                vm.pythonFile = null;
                vm.metadataFile = null;

                $scope.vm_createPipeline.isCreatingStep = false;
            }

        }).catch(function (error) {
            if (error && error.errMsg) {
                vm.errorMsg = error.errMsg;
            } else {
                vm.errorMsg = 'Unexpected error has occured. Please try again.';
            }

        }).finally(function () {

            vm.loading = false;

        });
    };

});
