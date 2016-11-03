angular.module('app.modelquality.controller.PipelineCtrl', [
    'app.modelquality.controller.PipelineStepCtrl'
])
.controller('PipelineCtrl', function ($scope, $state, $q, Pipelines, ModelQualityService) {

    var vm = this;
    angular.extend(vm, {
        labels: {
            ADD_STEP: 'Upload New Step',
            PIPELINES: 'All Pipelines',
            CREATE_NEW: 'Create New',
            STEPS: 'New Pipeline Steps',
            EMPTY_STEPS: 'This pipeline has 0 steps',
            PIPELINE_NAME: 'Pipeline Name',
            PIPELINE_DESCRIPTION: 'Pipeline Description',
            CANCEL: 'Cancel',
            SAVE: 'Save',
            LOAD_SAVING: 'Saving Pipeline...',
            COPY_OR_NEW: 'Copy an existing pipeline or create new'
        },
        pipelines: Pipelines.resultObj,
        selectedPipeline: null,
        pipeline: null,
        pipelineName: null,
        pipelineDescription: null,
        isCreatingStep: false,
        stepMetadata: null,
        loading: false,
        error: false,
        message: null
    });

    vm.selectPipeline = function (pipeline) {
        vm.reset();

        if (!pipeline) {
            vm.selectedPipeline = null;
            vm.pipeline = {
                pipeline_steps: []
            };
            vm.pipelineDescription = null;
        } else {
            vm.selectedPipeline = pipeline;
            vm.pipeline = angular.copy(pipeline);
            vm.pipelineDescription = vm.pipeline.description;
        }
    };

    vm.inspectMetadata = function (index) {
        vm.stepMetadata = vm.pipeline.pipeline_steps[index];
    };

    vm.clearStepMetadata = function () {
        vm.stepMetadata = null;
    };

    vm.addStep = function (step) {
        var indexOfStep = vm.pipelineIndexOfStep(step.Name);

        if (indexOfStep > 0) {
            vm.pipeline[indexOfStep] = step;
        } else {
            vm.pipeline.pipeline_steps.push(step);
        }
    };

    vm.pipelineIndexOfStep = function (stepName) {
        for (var i = 0; i < vm.pipeline.pipeline_steps.length; i++) {
            if (vm.pipeline.pipeline_steps[i].Name === stepName) {
                return i;
            }
        }

        return -1;
    };

    vm.createStep = function () {
        vm.isCreatingStep = true;
        vm.error = false;
        vm.message = null;
    };

    vm.cancelAddStep = function () {
        vm.isCreatingStep = false;
    };

    vm.savePipeline = function () {
        vm.error = false;
        vm.message = null;
        vm.loading = true;

        if (!vm.pipelineName) {
            vm.error = true;
            vm.message = 'Pipeline name is required';
            return;
        }

        var newPipeline = vm.pipeline.pipeline_steps.map(function (step) {
            if (step.isNewStep) {
                return {
                    pipeline_step_dir: step.pipeline_step_dir
                };
            } else {
                return {
                    pipeline_step: step.Name
                };
            }
        });

        ModelQualityService.CreatePipeline(vm.pipelineName, vm.pipelineDescription, newPipeline)
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
                vm.message = result.resultObj.name + ' has been created.';
                vm.pipelines.push(result.resultObj);

            }).catch(function (error) {
                vm.error = true;
                if (error && error.errMsg) {
                    vm.message = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
                } else {
                    vm.message = 'Unexpected error has occured. Please try again.';
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
        vm.pipelineDescription = null;
        vm.isCreatingStep = false;
        vm.stepMetadata = null;
        vm.loading = false;
        vm.message = null;
        vm.error = false;
    };

    vm.swapSteps = function (a, b) {
        var temp = vm.pipeline.pipeline_steps[a];
        vm.pipeline.pipeline_steps[a] = vm.pipeline.pipeline_steps[b];
        vm.pipeline.pipeline_steps[b] = temp;
    };

    vm.deleteStep = function (index) {
        var deleted = vm.pipeline.pipeline_steps.splice(index, 1);

        if (vm.stepMetadata === deleted[0]) {
            vm.clearStepMetadata();
        }
    };

});
