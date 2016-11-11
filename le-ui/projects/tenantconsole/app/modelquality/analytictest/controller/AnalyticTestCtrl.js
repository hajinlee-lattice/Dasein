angular.module('app.modelquality.controller.AnalyticTestCtrl', [
    'ui.bootstrap'
])
.controller('MultiSelectModalCtrl', function($scope, $uibModalInstance, options){
    $scope.title = options.title;
    $scope.options = options.options;
    $scope.selected = options.selected;

    options.context[options.key].forEach(function (item) {
        $scope.selected[item] = true;
    });

})
.controller('AnalyticTestCtrl', function ($scope, $uibModal, AnalyticTests, AnalyticPipelines, Datasets, AnalyticTestTypes, ModelQualityService) {

    var vm = this;
    angular.extend(vm, {
        labels: {
            NEW_TEST: 'Create Analytic Test',
            CANCEL_TEST: 'Cancel',
            SAVE_TEST: 'Save',
            LOAD_SAVING: 'Saving...',
            ANALYTIC_PIPELINE_NAMES: 'Analytic Pipelines',
            ANALYTIC_TEST_TYPE: 'Test Type',
            ANALYTIC_TEST_TAG: 'Test Tag',
            DATASET_NAMES: 'Datasets',
            NAME: 'Name'
        },
        error: false,
        loading: false,
        message: null,
        isCreatingTest: false,
        analyticTests: AnalyticTests.resultObj,
        analyticTest: BaseAnalyticTest(),
        entityOptions: {
            analytic_pipeline_names: AnalyticPipelines.resultObj,
            dataset_names: Datasets.resultObj,
            analytic_test_type: AnalyticTestTypes.resultObj
        },
        columns: [
            {key:'name', type: 'input'},
            {key:'dataset_names', type: 'multi'},
            {key:'analytic_pipeline_names', type: 'multi'},
            {key:'analytic_test_type', type: 'select'},
            {key:'analytic_test_tag', type: 'input'}
        ]
    });

    // pagination
    vm.cur = 1;
    vm.max = 10;
    vm.total = vm.analyticTests.length;

    vm.newTest = function () {
        vm.isCreatingTest = true;
    };

    vm.saveNewTest = function () {
        var testToSave = angular.copy(vm.analyticTest);

        if (!isValidateAnalyticTest(testToSave)) {
            vm.error = true;
            vm.message = 'Please fill out all fields';
            return;
        }

        vm.loading = true;
        ModelQualityService.CreateAnalyticTest(testToSave)
            .then(function (result) {
                vm.error = false;
                vm.message = 'Analytic test ' + testToSave.name + ' created';
                vm.analyticTests.push(testToSave);
                vm.reset();
            }).catch(function (error) {
                vm.error = true;
                if (error && error.errMsg) {
                    vm.message = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
                } else {
                    vm.message = 'Error creating analytic test';
                }
            }).finally(function () {
                  vm.loading = false;
            });
    };

    vm.cancelNewTest = function () {
        vm.reset();
    };

    vm.reset = function () {
        vm.isCreatingTest = false;
        vm.error = false;
        vm.message = null;
        vm.analyticTest = BaseAnalyticTest();
    };

    vm.multiSelect = function (analyticTest, key) {
        var options = vm.entityOptions[key];
        var selected = {};

        var modalInstance = $uibModal.open({
            animation: true,
            ariaLabelledBy: 'modal-title',
            ariaDescribedBy: 'modal-body',
            templateUrl: 'MultiSelectModal.html',
            controller: 'MultiSelectModalCtrl',
            controllerAs: '$ctrl',
            resolve: {
                options: function () {
                    return {
                        context: analyticTest,
                        key: key,
                        title: vm.labels[key.toUpperCase()],
                        options: options,
                        selected: selected
                    };
                }
            }
        });

        modalInstance.closed.then(function () {
            analyticTest[key] = [];

            for (var k in selected) {
                if (selected[k]) {
                    analyticTest[key].push(k);
                }
            }
        });
    };

    function isValidateAnalyticTest (analyticTest) {
      for (var entity in analyticTest) {
        if (angular.isArray(analyticTest[entity])) {
            if (analyticTest[entity].length === 0) {
                return false;
            }
        } else if (!analyticTest[entity]) {
          return false;
        }
      }

      return true;
    }

    function BaseAnalyticTest () {
        return {
            name: null,
            dataset_names: [],
            analytic_test_type: null,
            analytic_test_tag: null,
            analytic_pipeline_names: []
        };
    }

});
