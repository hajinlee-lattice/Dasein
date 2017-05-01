angular.module('app.modelquality.controller.AnalyticTestCtrl', [
    'ui.bootstrap',
    'app.modelquality.directive.MultiSelectCheckbox'
])
.controller('MultiSelectModalCtrl', function($scope, $uibModalInstance, options){
    $scope.title = options.title;
    $scope.options = options.options;
    $scope.selected = options.selected;
    $scope.readonly = options.readonly;

    options.context[options.key].forEach(function (item) {
        $scope.selected[item] = true;
    });
})
.controller('MultiInputModalCtrl', function($scope, $q, $uibModalInstance, ModelQualityService, options){

    $scope.title = options.title;
    $scope.options = options.options;
    $scope.typeOptions = options.typeOptions;

    $scope.selected = options.selected;
    $scope.datasetToCreate = BaseDataset();

    options.context[options.key].forEach(function (item) {
        $scope.selected[item] = true;
    });

    $scope.message = null;
    $scope.error = false;
    $scope.loading = false;

    $scope.clear = function () {
        $scope.datasetToCreate = BaseDataset();
    };

    $scope.create = function () {
        var tenantType = $scope.datasetToCreate.type,
            tenantId = $scope.datasetToCreate.tenant,
            modelId = $scope.datasetToCreate.id;

        $scope.loading = true;
        $scope.error = false;

        var datasetMsg = ' dataset for tenantType: ' + tenantType + ' , tenantId: ' + tenantId + ', modelId: ' + modelId;

        $scope.message = 'Creating' + datasetMsg;

        ModelQualityService.CreateDatasetFromTenant(tenantType, tenantId, modelId).then(function (result) {
            $scope.clear();

            return ModelQualityService.GetDatasetByName(result.resultObj.name);
        }).then(function (results) {
            var dataset = results.resultObj;

            if (!hasDataset(dataset.name)) {
                $scope.options.push(dataset);
                $scope.message = 'Created' + datasetMsg;
            } else {
                $scope.message = dataset.name + ' already exists';
            }

            $scope.selected[dataset.name] = true;
        }).catch(function (error) {
            $scope.error = true;
            if (error && error.errMsg) {
                $scope.message = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
            } else {
                $scope.message = 'Error creating' + datasetMsg;
            }
        }).finally(function () {
            $scope.loading = false;
        });
    };

    function hasDataset(datasetName) {
        for (var i = 0; i < $scope.options.length; i++) {
            if ($scope.options[i].name === datasetName) {
                return true;
            }
        }

        return false;
    }

    function BaseDataset() {
        return {
            type: null,
            tenant: null,
            id: null
        };
    }
})
.controller('AnalyticTestCtrl', function ($scope, $q, $uibModal, AnalyticTests, AnalyticPipelines, AnalyticTestTypes, Datasets, ModelQualityService) {

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
            NAME: 'Name',
            RUN: 'Run',
            ACTION: 'Action'
        },
        error: false,
        loading: false,
        message: null,
        modelRunStatus: null,
        isCreatingTest: false,
        analyticTests: AnalyticTests.resultObj,
        analyticTest: BaseAnalyticTest(),
        entityOptions: {
            analytic_pipeline_names: AnalyticPipelines.resultObj,
            analytic_test_type: AnalyticTestTypes.resultObj,
            dataset_names: Datasets.resultObj
        },
        types: {
            dataset_names: ['LPI', 'LP2']
        },
        columns: [
            {key:'name', type: 'input'},
            {key:'dataset_names', type: 'multiInput'},
            {key:'analytic_pipeline_names', type: 'multiSelect'},
            {key:'analytic_test_type', type: 'select'},
            {key:'analytic_test_tag', type: 'input'}
        ],
        // pagination
        cur: 1,
        pageSize: 10
    });

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

        ModelQualityService.CreateAnalyticTest(testToSave).then(function (result) {
            vm.error = false;
            vm.message = 'Analytic test ' + result.resultObj.name + ' created';
            vm.analyticTests.push(testToSave);
            vm.reset();
        }).catch(function (error) {
            vm.error = true;
            if (error && error.errMsg) {
                vm.message = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
            } else if (typeof error === 'string') {
                vm.message = error;
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

    vm.clearModelRunStatus = function () {
        vm.modelRunStatus = null;
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
                        selected: selected,
                        readonly: analyticTest.analytic_test_type === 'Production'
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

    vm.multiInput = function (analyticTest, key) {
        var options = vm.entityOptions[key];
        var typeOptions = vm.types[key];
        var selected = {};

        var modalInstance = $uibModal.open({
            animation: true,
            ariaLabelledBy: 'modal-title',
            ariaDescribedBy: 'modal-body',
            templateUrl: 'MultiInputModal.html',
            controller: 'MultiInputModalCtrl',
            controllerAs: '$ctrl',
            resolve: {
                options: function () {
                    return {
                        context: analyticTest,
                        key: key,
                        title: vm.labels[key.toUpperCase()],
                        options: options,
                        typeOptions: typeOptions,
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

    vm.runTest = function (analyticTestName) {
        vm.error = false;
        vm.message = 'Executed ' + analyticTestName;
        vm.modelRunStatus = null;

        ModelQualityService.ExecuteAnalyticTest(analyticTestName)
            .then(function (result) {
                var hasError = result.resultObj.reduce(function (acc, cur) {
                    return acc || !!cur.errorMessage;
                }, false);

                vm.modelRunStatus = JSON.stringify(result.resultObj, null, 2);

                if (hasError) {
                    var defer = $q.defer();
                    defer.reject();
                    return defer.promise;
                }
            }).catch(function (error) {
                vm.error = true;
                if (error && error.errMsg) {
                    vm.message = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
                } else {
                    vm.message = 'Error executing analytic test: ' + analyticTestName;
                }
            });
    };

    vm.selectChange = function (key, value) {
        if (key === 'analytic_test_type' && value === 'Production') {
            ModelQualityService.LatestAnalyticPipeline()
                .then(function (result) {
                    vm.analyticTest.analytic_pipeline_names = [result.resultObj.name];
                });
        }
    };

    function isValidateAnalyticTest (analyticTest) {
        for (var entity in analyticTest) {
            if (!analyticTest[entity]) {
              return false;
            }

            if (angular.isArray(analyticTest[entity])) {
                if (analyticTest[entity].length === 0) {
                    return false;
                }
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
