angular.module('app.modelquality.controller.PipelineCtrl', [
    'app.modelquality.controller.PipelineStepCtrl'
])
.controller('PipelineCtrl', function ($scope, $state, $q, AnalyticPipelines, ModelQualityService) {

    var vm = this;
    angular.extend(vm, {
        labels: {
            ADD_STEP: 'Upload New Step',
            PIPELINES: 'All Pipelines',
            STEPS: 'New Pipeline Steps',
            EMPTY_STEPS: 'This pipeline has 0 steps',
            PIPELINE_NAME: 'Pipeline Name',
            PIPELINE_DESCRIPTION: 'Pipeline Description',
            CANCEL: 'Clear',
            SAVE: 'Save',
            SPINNER_SAVING: 'Saving Pipeline...',
            SPINNER_FETCHING: 'Fetching Pipeline...'
        },
        analyticPipelineProps: [
            'name',
            'algorithm_name',
            'dataflow_name',
            'pipeline_name',
            'prop_data_name',
            'sampling_name'
        ],
        analyticPipelines: AnalyticPipelines.resultObj,
        selectedPipeline: null,
        pipelineSteps: [],
        analyticPipeline: {},
        pipelineName: null,
        pipelineDescription: null,
        isCreatingStep: false,
        stepMetadata: null,
        loading: false,
        error: false,
        message: null,
        spinnerMsg: null
    });

    vm.selectPipeline = function (analyticPipeline) {
        vm.reset();

        vm.loading = true;
        vm.spinnerMsg = vm.labels.SPINNER_FETCHING;

        ModelQualityService.GetPipelineByName(analyticPipeline.pipeline_name)
            .then(function (result) {
                vm.selectedPipeline = analyticPipeline;
                vm.analyticPipeline = angular.copy(vm.selectedPipeline);

                vm.pipelineSteps = result.resultObj.pipeline_steps;
                vm.pipelineDescription = result.resultObj.description;
            }).catch(function (error) {
                vm.error = true;
                if (error && error.errMsg) {
                    vm.message = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
                } else {
                    vm.message = 'Failed to get Pipeline of ' + analyticPipeline.name;
                }
            }).finally(function () {
                vm.loading = false;
            });
    };

    vm.inspectMetadata = function (index) {
        vm.stepMetadata = vm.pipelineSteps[index];
    };

    vm.clearStepMetadata = function () {
        vm.stepMetadata = null;
    };

    vm.addStep = function (step) {
        if (pipelineIndexOfStep(step.Name) < 0) {
            vm.pipelineSteps.push(step);
        }
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
        if (!vm.pipelineName) {
            vm.error = true;
            vm.message = 'Pipeline name is required';
            return;
        }

        vm.error = false;
        vm.message = null;
        vm.loading = true;
        vm.spinnerMsg = vm.labels.SPINNER_SAVING;

        var newPipeline = vm.pipelineSteps.map(function (step) {
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
                return ModelQualityService.GetPipelineByName(result.resultObj.pipelineName);
            }).then(function (result) {
                vm.analyticPipeline.pipeline_name = result.resultObj.name;

                saveAnalyticPipeline();
            }).catch(function (error) {
                vm.error = true;
                if (error && error.errMsg) {
                    vm.message = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
                } else {
                    vm.message = 'Unexpected error has occured. Please try again.';
                }
                vm.loading = false;
            });
    };

    vm.cancelPipeline = function () {
        vm.reset();
    };

    vm.reset = function () {
        vm.selectedPipeline = null;
        vm.pipelineName = null;
        vm.pipeline = [];
        vm.pipelineDescription = null;

        vm.isCreatingStep = false;
        vm.stepMetadata = null;
        vm.loading = false;
        vm.message = null;
        vm.error = false;
    };

    vm.swapSteps = function (a, b) {
        var temp = vm.pipelineSteps[a];
        vm.pipelineSteps[a] = vm.pipelineSteps[b];
        vm.pipelineSteps[b] = temp;
    };

    vm.deleteStep = function (index) {
        var deleted = vm.pipelineSteps.splice(index, 1);

        if (vm.stepMetadata === deleted[0]) {
            vm.clearStepMetadata();
        }
    };

    function saveAnalyticPipeline () {
        vm.analyticPipeline.name = vm.pipelineName;
        if (!isValidPipeline()) {
            vm.error = true;
            vm.message = 'Invalid pipeline';
            return;
        }

        ModelQualityService.CreateAnalyticPipeline(vm.analyticPipeline)
            .then(function (result) {
                vm.analyticPipelines.push(angular.copy(vm.analyticPipeline));

                vm.reset();
                vm.message = result.resultObj.pipelineName + ' has been created.';
            }).catch(function (error) {
                vm.error = true;

                if (error && error.errMsg) {
                  vm.message = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
                } else {
                  vm.message = 'Error creating analytic pipeline';
                }
            }).finally(function () {
                vm.loading = false;
            });
    }

    function pipelineIndexOfStep (stepName) {
        for (var i = 0; i < vm.pipelineSteps.length; i++) {
            if (vm.pipelineSteps[i].Name === stepName) {
                return i;
            }
        }

        return -1;
    }

    function isValidPipeline() {
      var valid = !!vm.pipelineName;

      vm.analyticPipelineProps.forEach(function (prop) {
        valid = valid && !!vm.analyticPipeline[prop];
      });

      return valid;
    }
});
