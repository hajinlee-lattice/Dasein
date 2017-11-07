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
        this.prospect = {'prospect':0, 'customers':0};
        this.productsSelected = {};


        this.init = function () {
            this.prospect = RatingsEngineAIService.getProspectCount();
            this.customers = RatingsEngineAIService.getCustomersCount();
            this.settings = {};

            this.buildOptions = RatingsEngineAIService.getBuildOptions();
        }

        this.init();

        this.clear = function () {
            this.init();
        }

        this.setProspect = function(prospect) {
            this.prospect = prospect;
        }

        this.loadProducts = function(pageNumber) {

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
            var c = Object.keys(this.productsSelected).length;
            return c;
        }

    })
    .service('RatingsEngineAIService', function ($q, $http, $state) {

        this.getProspectCount = function () {
            return 2000;
        };

        this.getCustomersCount = function () {
            return 5000;
        }

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
            var data = {'prospect':1500, 'customers':350};
            deferred.resolve(data);
            
    
            return deferred.promise;
        }
        /**
         * Load the products by page number
         */
        this.getProducts = function(page){
            var deferred = $q.defer();

            var data = [];
            var from = (page-1)*10;
            var to = page * 10;
            for(var i=from; i< to; i++){
                data.push({'selected':false, 'id':i, 'name': "Product Name "+i});
            }
            deferred.resolve(data);
            
    
            return deferred.promise;
        }
    });