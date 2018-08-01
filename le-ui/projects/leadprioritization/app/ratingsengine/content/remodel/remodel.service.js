angular.module('lp.ratingsengine.remodel')
.service('AtlasRemodelStore', function($q, $state, $stateParams, $timeout, BrowserStorageUtility, AtlasRemodelService) {
    var store = this;

    this.init = function(){
        this.filters = {
            page: 1,
            pagesize: 25,
            sortPrefix: '+',
            queryText: ''
        };

        this.remodelIteration = null;

        this.limit = -1;
        this.selected = [];
        this.start_selected = [];
        this.category = '';

        this.remodelAttributes = {};
    };

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

    this.saveIteration = function() {

        var deferred = $q.defer(),
            engineId = $stateParams.engineId,
            iteration = store.getRemodelIteration(),
            attributes = store.getRemodelAttributes(),
            clientSession = BrowserStorageUtility.getClientSession(),
            createdBy = clientSession.EmailAddress;

        // Sanitize attributes to remove OriginalApprovedUsage (used when toggling Enable/Disable in Attributes screen)
        angular.forEach(attributes, function(category){
            var modifiedAttributes = category.filter(attribute => attribute.OriginalApprovedUsage);
            if(modifiedAttributes.length > 0){
                angular.forEach(modifiedAttributes, function(attribute){
                    delete attribute.OriginalApprovedUsage; 
                });
            }
        });

        // Set sanitized attributes and created by properties prior to saving iteration
        iteration.AI.ratingmodel_attributes = attributes;
        iteration.AI.createdBy = createdBy;

        // Save iteration
        AtlasRemodelService.saveIteration(engineId, iteration).then(function(result){
            console.log(result);
            deferred.resolve(result);
        });

        return deferred.promise;

    };

    this.init();
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

});