angular.module('lp.ratingsengine.ai', [])
    .service('RatingsEngineAIStore', function (
        $q, $state, $stateParams, RatingsEngineService, RatingsEngineAIService, DataCloudStore,
        BrowserStorageUtility, SegmentStore, $timeout
    ) {

        this.buildOptions = [];

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


    });