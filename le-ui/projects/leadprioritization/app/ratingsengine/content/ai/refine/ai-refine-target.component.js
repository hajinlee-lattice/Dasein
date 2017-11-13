angular.module('lp.ratingsengine.ai.refine', ['mainApp.appCommon.directives.chips'])
    .controller('RatingsEngineAIRefineTarget', function ($scope, RefineService) {
        var vm = this;

        angular.extend(vm, {
            refine: RefineService.refineModel,
            sellOption: '',
            notyet: false,
            resell: false

        });

        vm.init = function () {
            RefineService.reset();
            console.log('Init refine Target');
            $scope.$watch(function () {
                return RefineService.refineModel;
            },
                function (newVal, oldVal) {
                    // alert("Inside watch");
                    vm.refine = newVal;
                    console.log('NEW ' + newVal + ' - OLD ' + oldVal);
                }, true);


        }

        vm.showRefineModel = function () {
            console.log('Change view');
            RefineService.changeValue();
        }
        vm.init();
    })
    .controller('RatingsEngineAIRefineModel', function ($scope, RefineService) {
        var vm = this;
        angular.extend(vm, {
            refine: RefineService.refineModel,
            spent: false,
            bought: false,
            historical: false,
            similarProducts: false,
            similarSegments: false,
            productDataSource: [],
            datasource: []

        });

        vm.init = function () {
            console.info('Init refine Model');
            $scope.$watch(function () {
                return RefineService.refineModel;
            },
                function (newVal, oldVal) {
                    // alert("Inside watch");
                    vm.refine = newVal;
                    console.log('NEW ' + newVal + ' - OLD ' + oldVal);
                }, true);
            vm.productsDataSource =
                [
                    { 'id': 1, 'name': 'iPhone6' },
                    { 'id': 2, 'name': 'iPhone6+' },
                    { 'id': 3, 'name': 'iPhone7' },
                    { 'id': 4, 'name': 'iPhone7+' },
                    { 'id': 5, 'name': 'iPhoneX' },
                    { 'id': 6, 'name': 'Nokya' },
                    { 'id': 7, 'name': 'Samsung7' },
                    { 'id': 8, 'name': 'Google Phone' }
                ];
            vm.datasource =
                [
                    { 'id': 1, 'name': 'New Yourk' },
                    { 'id': 2, 'name': 'Russian' },
                    { 'id': 3, 'name': 'Italy' },
                    { 'id': 4, 'name': 'Florence' },
                    { 'id': 5, 'name': 'Rome' },
                    { 'id': 6, 'name': 'London' },
                    { 'id': 7, 'name': 'Paris' },
                    { 'id': 8, 'name': 'Madrid' },
                    { 'id': 9, 'name': 'Instambul' },
                    { 'id': 10, 'name': 'Tokyo' },
                    { 'id': 11, 'name': 'San Jose' },
                    { 'id': 12, 'name': 'San Francisco' }
                ];
        }

        vm.callbackSegments = function (element) {
            console.log(element);
            // console.log(element.args);
        }
        vm.productsCallback = function (elements) {
            console.log(elements);
        }

        vm.init();
    })
    .service('RefineService', function () {
        this.refineModel = false;

        this.reset = function () {
            this.refineModel = false;
        }

        this.changeValue = function () {
            this.refineModel = !this.refineModel;
        }
        this.getRefineModel = function () {
            return this.refineModel;
        }
    });