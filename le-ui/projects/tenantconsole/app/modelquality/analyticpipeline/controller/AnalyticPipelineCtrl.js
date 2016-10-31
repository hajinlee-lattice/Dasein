angular.module('app.modelquality.controller.AnalyticPipelineCtrl', [
])
.controller('AnalyticPipelineCtrl', function ($scope, $state, $q, Algorithms, AnalyticPipelines, Pipelines, SamplingConfigs, Dataflows, PropdataConfigs, ModelQualityService) {

    var vm = this;
    angular.extend(vm, {
        labels: {
            ANALYTIC_PIPELINES: 'All Analytic Pipelines',
            NEW_ANALYTIC_PIPELINE: 'New Analytic Pipeline',
            PIPELINE_NAME: 'Analytic Pipeline Name',
            CLEAR: 'Clear',
            SAVE: 'Save',
            LOAD_SAVING: 'Saving...'
        },
        loading: false,
        analyticPipelines: AnalyticPipelines.resultObj,
        selectedPipeline: null,
        pipeline: {},
        propertyOptions: {
            algorithm_name: Algorithms.resultObj,
            dataflow_name: Dataflows.resultObj,
            pipeline_name: Pipelines.resultObj,
            prop_data_name: PropdataConfigs.resultObj,
            sampling_name: SamplingConfigs.resultObj
        },
        message: null,
        error: false
    });

    vm.props = [{key:'algorithm_name', displayName: 'Algorithm Name'},
                {key: 'dataflow_name', displayName: 'Dataflow Name'},
                {key: 'pipeline_name', displayName: 'Pipeline Name'},
                {key: 'prop_data_name', displayName: 'Prop Data Name'},
                {key: 'sampling_name', displayName: 'Sampling Name'}];

    vm.selectPipeline = function (pipeline) {
        if (!pipeline) {
            vm.selectedPipeline = null;
            vm.pipeline = {};
        } else {
            vm.selectedPipeline = pipeline;
            vm.pipeline = angular.copy(pipeline);
            vm.pipeline.name = null;
        }
    };

    vm.savePipeline = function () {
        vm.loading = true;
        vm.error = false;
        vm.message = null;


        if (!vm.validatePipeline()) {
          vm.loading = false;
          vm.error = true;
          vm.message = 'All fields are required';
          return;
        }

        ModelQualityService.CreateAnalyticPipeline(vm.pipeline)
            .then(function (result) {
                vm.analyticPipelines.push(vm.pipeline);
                vm.clearPipeline();
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
    };

    vm.clearPipeline = function () {
      vm.error = false;
      vm.message = false;
      vm.pipeline = {};
    };

    vm.validatePipeline = function () {
      var valid = !!vm.pipeline.name;
      
      vm.props.forEach(function (prop) {
        valid = valid && !!vm.pipeline[prop.key];
      });

      return valid;
    };

});
