angular.module('app.modelquality.controller.ModelQualityCreatePipelineCtrl', [
    'app.modelquality.controller.ModelQualityCreatePipelineStepCtrl'
])
.controller('ModelQualityCreatePipelineCtrl', function ($scope, $state, $q, Pipelines, ModelQualityService) {

    var vm = this;
    angular.extend(vm, {
        labels: {
            ADD_STEP: 'Upload New Step',
            PIPELINES: 'All Model Pipelines',
            CREATE_NEW: 'Create New',
            STEPS: 'New Pipeline Steps',
            EMPTY_STEPS: 'This pipeline has 0 steps',
            PIPELINE_NAME: 'Pipeline Name',
            CANCEL: 'Cancel',
            SAVE: 'Save',
            LOAD_SAVING: 'Saving Pipeline...',
            COPY_OR_NEW: 'Copy an existing pipeline or create new'
        },
        pipelines: Pipelines.resultObj,
        selectedPipeline: null,
        pipeline: null,
        pipelineName: null,
        isCreatingStep: false,
        errorMsg: null,
        loading: false,
        stepMetadata: null
    });

    vm.selectPipeline = function (pipeline) {
        if (!pipeline) {
            vm.selectedPipeline = null;

            vm.pipeline = {
                pipeline_steps: []
            };
        } else {
            vm.selectedPipeline = pipeline;
            vm.pipeline = angular.copy(pipeline);
        }
    };

    vm.inspectMetadata = function (index) {
        vm.stepMetadata = vm.pipeline.pipeline_steps[index];
    };

    vm.clearStepMetadata = function () {
        vm.stepMetadata = null;
    };

    vm.addStep = function (step) {
        vm.pipeline.pipeline_steps.push(step);
    };

    vm.createStep = function () {
        vm.isCreatingStep = true;
    };

    vm.cancelAddStep = function () {
        vm.isCreatingStep = false;
    };

    vm.savePipeline = function () {
        if (!vm.pipelineName) {
            vm.errorMsg = 'Pipeline name is required';
            return;
        }

        vm.errorMsg = null;
        vm.successMsg = null;
        var newPipeline = vm.pipeline.pipeline_steps.map(function (step) {
            return {
                pipeline_step: step.Name,
                pipeline_step_dir: null
            };
        });

        vm.loading = true;
        ModelQualityService.CreatePipeline(vm.pipelineName, newPipeline)
            .then(function (result) {

                if (result.resultObj) {
                    return ModelQualityService.GetPipelineByName(result.resultObj.pipelineName);
                } else {
                    var defer = $q.defer();
                    defer.reject(result);
                    return defer.promise;
                }

            }).then(function (result) {

                vm.reset();
                vm.successMsg = result.resultObj.name + ' has been created.';
                vm.pipelines.push(result.resultObj);

            }).catch(function (error) {

                if (error && error.errMsg) {
                    vm.errorMsg = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
                } else {
                    vm.errorMsg = 'Unexpected error has occured. Please try again.';
                }

            }).finally(function () {

                vm.loading = false;

            });

    };

    vm.cancelPipeline = function () {
        vm.reset();
    };

    vm.reset = function () {
        vm.selectedPipeline = null;
        vm.pipeline = null;
        vm.pipelineName = null;
        vm.isCreatingStep = false;
        vm.errorMsg = null;
        vm.successMsg = null;
        vm.loading = false;
        vm.stepMetadata = null;
    };

    vm.swapSteps = function (a, b) {
        var temp = vm.pipeline.pipeline_steps[a];
        vm.pipeline.pipeline_steps[a] = vm.pipeline.pipeline_steps[b];
        vm.pipeline.pipeline_steps[b] = temp;
    };

    vm.deleteStep = function (index) {
        vm.pipeline.pipeline_steps.splice(index, 1);
    };

});
