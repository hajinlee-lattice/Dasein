angular.module('lp.ratingsengine.remodel')
.service('AtlasRemodelStore', function($q, $state, $stateParams, $timeout, BrowserStorageUtility, AtlasRemodelService, RatingsEngineStore, JobsStore) {
    var store = this;

    this.init = function(){

        console.log("init remodel store");

        this.remodelIteration = null;

        this.filters = {
            currentPage: 1,
            pageSize: 10,
            sortPrefix: '+',
            queryText: ''
        };

        this.limit = -1;
        this.selected = [];
        this.start_selected = [];
        this.category = '';

        this.remodelAttributes = {};
    };

    this.init();

    this.clear = function() {
        this.init();
    }

    this.set = function(property, value) {
        this[property] = value;
    };
    this.get = function(property) {
        return this[property];
    };

    this.getFilters = function() {
        return this.filters;
    }

    this.setRemodelIteration = function(iteration) {
        this.remodelIteration = iteration;
    }
    this.getRemodelIteration = function() {
        return this.remodelIteration;
    }

    this.getRemodelAttributes = function(){
        return this.remodelAttributes;
    }
    this.setRemodelAttributes = function(remodelAttributes){
        this.remodelAttributes = remodelAttributes;
    }

    this.getAttributes = function(engineId, modelId) {
        var deferred = $q.defer();

        AtlasRemodelService.getAttributes(engineId, modelId).then(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.saveIteration = function(nextState) {

        var engineId = $stateParams.engineId,
            iteration = store.getRemodelIteration(),
            clientSession = BrowserStorageUtility.getClientSession(),
            createdBy = clientSession.EmailAddress;

        iteration.AI.derived_from_rating_model = iteration.AI.id;
        iteration.AI.createdBy = createdBy;

        if(iteration.AI.advancedModelingConfig.cross_sell){
            iteration.AI.advancedModelingConfig.cross_sell.filters = RatingsEngineStore.getConfigFilters();
        } else {
            iteration.AI.advancedModelingConfig.custom_event = RatingsEngineStore.getConfigFilters();
        }

        // Sanitize iteration to remove data
        delete iteration.AI.pid;
        delete iteration.AI.id;
        delete iteration.AI.modelingJobId;
        delete iteration.AI.modelingJobStatus;
        delete iteration.AI.modelSummaryId;

        console.log(iteration);
        

        // Save iteration
        AtlasRemodelService.saveIteration(engineId, iteration).then(function(result){
            
            console.log(result);

            var modelId = result.AI.id,
                attributes = store.getRemodelAttributes();

            // Sanitize attributes to remove OriginalApprovedUsage (used when toggling Enable/Disable in Attributes screen)
            angular.forEach(attributes, function(category){
                var modifiedAttributes = category.filter(function(attribute) {
                    return attribute.OriginalApprovedUsage;
                });
                if(modifiedAttributes.length > 0){
                    angular.forEach(modifiedAttributes, function(attribute){
                        delete attribute.OriginalApprovedUsage; 
                    });
                }
                var hasWarningAttributes = category.filter(function(attribute) {
                    return attribute.hasWarning;
                });
                if(hasWarningAttributes.length > 0){
                    angular.forEach(hasWarningAttributes, function(attribute){
                        delete attribute.hasWarning; 
                    });
                }
            });

            // Launch Model
            AtlasRemodelService.launchModeling(engineId, modelId, attributes).then(function(applicationid){
                console.log(applicationid);

                RatingsEngineStore.setApplicationId(applicationid);
                JobsStore.inProgressModelJobs[engineId] = null;

                // console.log('Model Launched', id, nextState);
                if(nextState) {
                    $state.go(nextState, { ai_model_job_id: applicationid });
                }
            });


        });

    };


}) 
.service('AtlasRemodelService', function($q, $http) {

    this.getAttributes = function(engineId, modelId){
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url:  '/pls/ratingengines/' + engineId + '/ratingmodels/' + modelId + '/metadata'
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
    
        return deferred.promise;
    }

    this.saveIteration = function(engineId, iteration){
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/' + engineId + '/ratingmodels',
            data: iteration
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.launchModeling = function(engineId, modelId, attributes){
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/' + engineId + '/ratingmodels/' + modelId + '/model',
            headers: {
                'Accept': 'text/plain'
            },
            data: attributes
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

});