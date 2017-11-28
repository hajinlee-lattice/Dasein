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
            vm.validateNextStep();
            // console.log('Init refine Target');
            $scope.$watch(function () {
                return RefineService.refineModel;
            },
                function (newVal, oldVal) {
                    vm.refine = newVal;
                }, true);
            vm.resellOption = vm.resellOptions[0];
            vm.sellTypeChosen('new');
            vm.prioritizeOptionChosen('PROPENSITY');

        }

        /**
         * 
         * @param value 
         * Set the type of sell chosen
         */
        vm.sellTypeChosen = function (value) {
            // console.log('Changed ' + value);
            vm.sellType = value;
            if ('new' === value) {
                RatingsEngineAIStore.setSellOption(value, {});
                vm.getProspectCustomers();
            } else {
                RatingsEngineAIStore.setSellOption(value, vm.resellOption);
                vm.getProspectCustomers();
            }
            vm.validateNextStep();
        }

        /**
         * 
         * @param value 
         * Set the type of prioritization chosen
         */
        vm.prioritizeOptionChosen = function (value) {
            // console.log('Prioritize', value);
            vm.prioritizeOption = value;
            RatingsEngineAIStore.setPrioritizeOption(value);
            vm.getProspectCustomers();
            vm.validateNextStep();
        }

        vm.getProspectCustomers = function () {

            RatingsEngineAIStore.getProspectCustomers().then(function (result) {
                vm.customers = result.customers;
                vm.prospects = result.prospects;
            });
        }

        /**
         * Switch for the refine model view
         */
        vm.showRefineModel = function () {
            // console.log('Change view');
            RefineService.changeValue();
        }



         /**
         * Method used to validate the next step for the wizard
         */
        vm.validateNextStep = function () {
            if (!angular.equals(RatingsEngineAIStore.sellOption, {}) && !angular.equals(RatingsEngineAIStore.prioritizeOption, {})) {
                vm.setValidation('refine', true);
            } else {
                vm.setValidation('refine', false);
            }
        }

         /**
         * Enable/Disable the next step of the wizard
         * @argument type @type string This value has to be equal to the value which is inside the RatingsEngineStore.validation
         * @argument validated @type boolean Enable or disable the next step
         * 
         */
        vm.setValidation = function (type, validated) {
            RatingsEngineStore.setValidation(type, validated);
        }


        vm.init();
    })
    .controller('RatingsEngineAIRefineModel', function ($scope, RefineService, RatingsEngineStore, RatingsEngineAIStore, RatingsEngineAIService, Products, Segments) {
        var vm = this;
        // console.log('PRODUCTS',Products);
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
                    ret.push({'id': Products[i].ProductId, 'displayName': Products[i].ProductName});
                }
                return ret;
            })(),
            segmentsDatasource: (function(){
                var max = Segments.length;
                var ret = [];
                for(var i=0; i<max; i++){
                    if(''!== Segments[i].name) {
                        ret.push({'id': Segments[i].name, 'displayName': Segments[i].display_name});
                    }
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
            // console.info('Init refine Model');
            $scope.$watch(function () {
                return RefineService.refineModel;
            },
                function (newVal, oldVal) {
                    vm.refine = newVal;
                    // console.log('NEW ' + newVal + ' - OLD ' + oldVal);
                }, true);

         
            vm.datasource = [];

            vm.getProspectCustomers();

        }

        vm.similarProductsChecked = function() {
            console.log(vm.similarProducts);
            if(!vm.similarProducts){
                RatingsEngineAIStore.addSimilarProducts({});
                // RatingsEngineStore.nextSaveProductToAIModel();
            }
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
            // console.log(element);
        }
        vm.productsCallback = function (elements) {
            RatingsEngineAIStore.addSimilarProducts(elements);
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