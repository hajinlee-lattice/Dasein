angular.module('lp.ratingsengine.ai', [])
    .config(['$httpProvider', function ($httpProvider) {
        $httpProvider.defaults.useXDomain = true;
        delete $httpProvider.defaults.headers.common['X-Requested-With'];
    }
    ])
    .service('RatingsEngineAIStore', function (
        $q, $state, $stateParams, RatingsEngineService, RatingsEngineAIService, DataCloudStore,
        BrowserStorageUtility, $timeout, SegmentService
    ) {

        this.buildOptions = [];

        this.productsSelected = {};
        this.similarProducts = {};
        this.trainingProducts = []; // this is the value to add to the AI model

        this.aiModelOptions = {
            workflowType: 'CROSS_SELL',
            targetCustomerSet: 'new',
            modelingMethod: 'PROPENSITY',
            trainingProducts: [],
            modelingJobId: '',
            similarSegmentId: ''
        };

        /************* Refine variables *************/
        this.customers = '',
        this.successes = '',

        this.sellOption = {};
        this.prioritizeOption = {};
        /*******************************************/



        this.init = function () {
            this.prospect = { 'prospect': 0, 'customers': 0 };
            this.settings = {};

            this.buildOptions = RatingsEngineAIService.getBuildOptions();
        }

        this.init();

        this.clear = function () {
            this.init();
        }

        this.getNumberOfProducts = function () {
            RatingsEngineAIService.getProductsCount(segment_id).then(function (data) {
                deferred.resolve(data);
            });

            return deferred.promise;
        }

        this.clearSelection = function () {
            this.productsSelected = {};
        }

        this.selectProduct = function (id, name) {
            if (this.productsSelected[id]) {
                delete this.productsSelected[id];
            } else {
                this.productsSelected[id] = name;
            }
        }
        this.getProductsSelected = function () {
            return this.productsSelected;
        }
        this.isProductSelected = function (id) {
            if (this.productsSelected[id]) {
                return true;
            } else {
                return false;
            }
        }
        this.getProductsSelectedCount = function () {
            return Object.keys(this.productsSelected).length;
        }

        this.getProducts = function (params) {
            var deferred = $q.defer();
            RatingsEngineAIService.getProductsAPI(params).then(function (data) {
                deferred.resolve(data.data);
            });
            return deferred.promise;

        }

        this.getSegments = function(){
            var deferred = $q.defer();
            SegmentService.GetSegments().then(function (data) {
                deferred.resolve(data);
            });
            return deferred.promise;
        }



        /**
         * Reset the option for the refine
         */
        this.resetRefineOptions = function () {
            this.sellOption = {};
            this.prioritizeOption = {};
        }
        /**
         * 
         * @param value @type Object
         * {'sellValue': '', sellOptions: {'name': ''}}
         * Store the value choosen for the refine the target
         *  
         */
        this.setSellOption = function (sellValue, options) {
            this.sellOption['sellValue'] = sellValue;
            this.sellOption['sellOptions'] = options;
        }



        this.getProspectCustomers = function () {
            var deferred = $q.defer();
            if (!angular.equals(this.sellOption, {}) && !angular.equals(this.prioritizeOption, {})) {
               

                RatingsEngineAIService.getProspectsCustomers(this.sellOption, this.prioritizeOption).then(function (response) {
                    deferred.resolve(response);
                });

                return deferred.promise;
            }else{
                return deferred.promise;
            }
        };

        /**
         * 
         * @param value @type string
         * Store the option to prioritize the target refine 
         */
        this.setPrioritizeOption = function (value) {
            this.prioritizeOption[value] = value;
        }

        this.getProductsSelectedIds = function() {
            var ids = Object.keys(this.productsSelected);
            return ids;
        }

        this.addSimilarProducts = function(products) {
            this.similarProducts = products;
        }

        this.getProdutTrainingIds = function(){
            return Object.keys(this.similarProducts);
        }


    })
    .service('RatingsEngineAIService', function ($q, $http, $state) {

        this.getBuildOptions = function () {
            var buildOptions = [
                { id: 0, label: 'Rate Customers likely to buy next quarter', name: '', disabled: 'false' },
                { id: 1, label: 'Rate Prospects for fit to my Products', name: '', disabled: 'true' },
                { id: 2, label: 'Rate Prospect for overall fit to my businnes', name: '', disabled: 'true' },
                { id: 3, label: 'Upload training data and build a custom probability model', name: '', disabled: 'true' },
                { id: 4, label: 'Upload a model I already built (PMML format)', name: '', disabled: 'true' }
            ];
            return buildOptions;
        }

        this.getSellOptions = function () {
            var deferred = $q.defer();
            var data = [
                { 'id': 1, 'name': '6 months' },
                { 'id': 2, 'name': '12 months' },
                { 'id': 3, 'name': '18 months' }
            ];
            deferred.resolve(data);


            return deferred.promise;
        }
        /**
         * Fetch the prospect count and the customers cound given a segment id
         */
        this.getProspect = function (id) {
            var deferred = $q.defer();
            var data = { 'prospect': 500, 'customers': 150 };
            deferred.resolve(data);


            return deferred.promise;
        }
        /**
         * Return the number of product
         */
        this.getProductsCount = function () {
            var deferred = $q.defer();
            var data = { 'count': 100 };
            deferred.resolve(data);
            return deferred.promise;
        }


        /**
       * Load the products by page number
       */
        this.getProductsAPI = function (params) {
            var deferred = $q.defer();
            var max = params.max;
            var offset = params.offset;
            var data = [];
            url = '/pls/products/data';
            $http({
                method: 'GET',
                url: url,
                params: {
                    max: params.max || 1000,
                    offset: params.offset || 0
                },
                headers: {
                    'Accept': 'application/json'
                }
            }).then(function (response) {
                deferred.resolve(response.data);
            }, function (response) {
                deferred.resolve(response.data);
            });
            return deferred.promise;
        }
        /**
         * Load the products by page number
         */
        this.getProducts = function (from, to) {
            var deferred = $q.defer();
            var data = [];
            for (var i = from; i < to; i++) {
                data.push({ 'selected': false, 'id': i, 'name': "Product Name " + i });
            }
            deferred.resolve(data);


            return deferred.promise;
        }

        this.getProspectsCustomers = function (sellOption, prioritizeOption) {
            console.log('API CALL');
            var deferred = $q.defer();
            var data = { 'prospects': Math.floor(Math.random() * 10000), 'customers': Math.floor(Math.random() * 10000) };

            deferred.resolve(data);


            return deferred.promise;
        }
    });