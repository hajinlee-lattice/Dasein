angular.module('lp.ratingsengine.ai.refine', ['mainApp.appCommon.directives.chips', 'mainApp.appCommon.directives.input.selection'])
    .controller('RatingsEngineAIRefineTarget', function ($scope, RefineService, RatingsEngineStore, RatingsEngineAIStore, RefineSellOptions) {
        var vm = this;

        angular.extend(vm, {
            refine: RefineService.refineModel,

            customers: '',
            prospects: '',
            historical: '',

            sellType: '',
            resellOptions: RefineSellOptions,
            resellOption: {},
            prioritizeOption: ''

        });

        vm.init = function () {
            RefineService.reset();
            RatingsEngineAIStore.resetRefineOptions();
            console.log('Init refine Target');
            $scope.$watch(function () {
                return RefineService.refineModel;
            },
                function (newVal, oldVal) {
                    vm.refine = newVal;
                }, true);
            vm.resellOption = vm.resellOptions[0];

        }

        /**
         * 
         * @param value 
         * Set the type of sell chosen
         */
        vm.sellTypeChosen = function (value) {
            console.log('Changed ' + value);
            if ('sell' === value) {
                RatingsEngineAIStore.setSellOption(value, {});
                vm.getProspectCustomers();
            } else {
                RatingsEngineAIStore.setSellOption(value, vm.resellOption);
                vm.getProspectCustomers();
            }
        }

        /**
         * 
         * @param value 
         * Set the type of prioritization chosen
         */
        vm.prioritizeOptionChosen = function (value) {
            console.log('Prioritize', value);
            RatingsEngineAIStore.setPrioritizeOption(value);
            vm.getProspectCustomers();
        }

        vm.getProspectCustomers = function () {

            RatingsEngineAIStore.getProspectCustomers().then(function (result) {
                vm.customers = result.customers;
                vm.prospects = result.prospects;
            });
        }

        vm.setValidation = function (type, validated) {
            console.log(type, validated);
            RatingsEngineStore.setValidation(type, validated);
        }

        /**
         * Switch for the refine model view
         */
        vm.showRefineModel = function () {
            console.log('Change view');
            RefineService.changeValue();
        }
        vm.init();
    })
    .controller('RatingsEngineAIRefineModel', function ($scope, RefineService, RatingsEngineAIStore, RatingsEngineAIService, Products) {
        var vm = this;
        console.log('PRODUCTS',Products);
        angular.extend(vm, {
            refine: RefineService.refineModel,

            customersCount: '',
            prospectsCount: '',
            historicalCount: '',


            spent: false,
            bought: false,
            historical: false,
            similarProducts: false,
            similarSegments: false,
            productsDataSource: (function(){
                var max = Products.length;
                var ret = [];
                for(var i=0; i<max; i++){
                    ret.push({'id': i, 'displayName': Products[i].ProductName});
                }
                return ret;
            })(),
            
            datasource: [],
            options: [],
            spentOptions: [{ 'id': 1, 'name': 'At least' }, { 'id': 2, 'name': 'At most' }],
            boughtOptions: [{ 'id': 1, 'name': 'At least' }, { 'id': 2, 'name': 'At most' }],
            historicalOptions: [{ 'id': 1, 'name': 'Months' }, { 'id': 2, 'name': 'Years' }]

        });

        vm.init = function () {
            console.info('Init refine Model');
            $scope.$watch(function () {
                return RefineService.refineModel;
            },
                function (newVal, oldVal) {
                    vm.refine = newVal;
                    console.log('NEW ' + newVal + ' - OLD ' + oldVal);
                }, true);

         
            vm.datasource = [];

            vm.getProspectCustomers();

        }
        vm.getSpentOptions = function () {
            return vm.spentOptions;
        }
        vm.getBoughtOptions = function () {
            return vm.boughtOptions;
        }

        vm.getHistoricalOptions = function () {
            return vm.historicalOptions;
        }

        vm.callbackSegments = function (element) {
            console.log(element);
        }
        vm.productsCallback = function (elements) {
            console.log(elements);
        }

        vm.getProspectCustomers = function () {

            RatingsEngineAIStore.getProspectCustomers().then(function (result) {
                vm.customersCount = result.customers;
                vm.prospectsCount = result.prospects;
            });
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