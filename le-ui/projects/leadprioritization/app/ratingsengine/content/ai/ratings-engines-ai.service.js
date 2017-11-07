angular.module('lp.ratingsengine.ai', [])
    .config(['$httpProvider', function ($httpProvider) {
        $httpProvider.defaults.useXDomain = true;
        delete $httpProvider.defaults.headers.common['X-Requested-With'];
    }
    ])
    .service('RatingsEngineAIStore', function (
        $q, $state, $stateParams, RatingsEngineService, RatingsEngineAIService, DataCloudStore,
        BrowserStorageUtility, SegmentStore, $timeout
    ) {

        this.buildOptions = [];
        
        this.productsSelected = {};


        this.init = function () {
            this.prospect = {'prospect':0, 'customers':0}; 
            this.settings = {};

            this.buildOptions = RatingsEngineAIService.getBuildOptions();
        }

        this.init();

        this.clear = function () {
            this.init();
        }

        this.getNumberOfProducts = function() {
            RatingsEngineAIService.getProductsCount(segment_id).then(function(data) {
                deferred.resolve(data);
            });

            return deferred.promise;
        }
        
        this.clearSelection = function() {
            this.productsSelected = {};
        }

        this.selectProduct = function(id, name) {
            if(this.productsSelected[id]){
                delete this.productsSelected[id];
            }else {
                this.productsSelected[id] = name;
            }
        }
        this.getProductsSelected = function(){
            return this.productsSelected;
        }
        this.isProductSelected = function(id){
            if(this.productsSelected[id]){
                return true;
            }else {
                return false;
            }
        }
        this.getProductsSelectedCount = function(){
            return Object.keys(this.productsSelected).length;
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

        /**
         * Fetch the prospect count and the customers cound given a segment id
         */
        this.getProspect = function(id) {
            var deferred = $q.defer();
            var data = {'prospect':500, 'customers':150};
            deferred.resolve(data);
            
    
            return deferred.promise;
        }
        /**
         * Return the number of product
         */
        this.getProductsCount = function() {
            var deferred = $q.defer();
            var data = {'count': 100};
            deferred.resolve(data);
            return deferred.promise;
        }

        /**
         * Load the products by page number
         */
        this.getProducts = function(from, to){
            var deferred = $q.defer();
            var data = [];
            for(var i=from; i< to; i++){
                data.push({'selected':false, 'id':i, 'name': "Product Name "+i});
            }
            deferred.resolve(data);
            
    
            return deferred.promise;
        }
    });