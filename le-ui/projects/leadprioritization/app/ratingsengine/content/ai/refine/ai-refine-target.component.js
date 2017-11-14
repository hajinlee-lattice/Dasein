angular.module('lp.ratingsengine.ai.refine', ['mainApp.appCommon.directives.chips', 'mainApp.appCommon.directives.input.selection'])
    .controller('RatingsEngineAIRefineTarget', function ($scope, RefineService) {
        var vm = this;

        angular.extend(vm, {
            refine: RefineService.refineModel,
            sellOption: '',
            notyet: false,
            resell: false,
            resellOptions : [{'id': 1, 'name':'6 months'}, {'id': 2, 'name':'12 months'}, {'id': 3, 'name':'18 months'}]

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
            datasource: [],
            options: [],
            spentOptions : [{'id': 1, 'name':'At least'}, {'id': 2, 'name':'At most'}],
            boughtOptions: [{'id': 1, 'name':'At least'}, {'id': 2, 'name':'At most'}],
            historicalOptions: [{'id': 1, 'name':'Months'}, {'id': 2, 'name':'Years'}]

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
                    { 'id': 1, 'displayName': 'iPhone6' },
                    { 'id': 2, 'displayName': 'iPhone6+' },
                    { 'id': 3, 'displayName': 'iPhone7' },
                    { 'id': 4, 'displayName': 'iPhone7+' },
                    { 'id': 5, 'displayName': 'iPhoneX' },
                    { 'id': 6, 'displayName': 'Nokya' },
                    { 'id': 7, 'displayName': 'Samsung7' },
                    { 'id': 8, 'displayName': 'Google Phone' }
                ];
            vm.datasource =
                [
                    { 'id': 1, 'displayName': 'New Yourk' },
                    { 'id': 2, 'displayName': 'Russian' },
                    { 'id': 3, 'displayName': 'Italy' },
                    { 'id': 4, 'displayName': 'Florence' },
                    { 'id': 5, 'displayName': 'Rome' },
                    { 'id': 6, 'displayName': 'London' },
                    { 'id': 7, 'displayName': 'Paris' },
                    { 'id': 8, 'displayName': 'Madrid' },
                    { 'id': 9, 'displayName': 'Instambul' },
                    { 'id': 10, 'displayName': 'Tokyo' },
                    { 'id': 11, 'displayName': 'San Jose' },
                    { 'id': 12, 'displayName': 'San Francisco' }
                ];

        }
        vm.getSpentOptions = function(){
            return vm.spentOptions;
        }
        vm.getBoughtOptions = function(){
            return vm.boughtOptions;
        }

        vm.getHistoricalOptions = function(){
            return vm.historicalOptions;
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