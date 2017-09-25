angular.module('lp.ratingsengine')
.service('RatingsEngineStore', function($q, $state, $stateParams, RatingsEngineService, BrowserStorageUtility){
    var RatingsEngineStore = this;

    this.init = function() {
        this.settings = {};
        this.validation = {
            segment: true,
            attributes: false,
            rules: true
        }
        this.currentRating = {};
        this.rating = null;
        this.ratings = null;
        this.type = null;
        this.savedSegment = "";
    }

    this.init();
    
    this.clear = function() {
        this.init();
    }

    this.getValidation = function(type) {
        return this.validation[type];
    }

    this.setValidation = function(type, value) {
        this.validation[type] = value;
    }

    this.setSettings = function(obj) {
        var obj = obj || {};

        for (var i in obj) {
            var key = i,
                value = obj[i];
            this.settings[key] = value;
        }
    }

    this.nextSaveGeneric = function(nextState) {
        var changed = false,
            opts = RatingsEngineStore.settings;
        
        $state.go(nextState, {rating_id: $stateParams.rating_id});

        // RatingsEngineStore.saveRating().then(function(rating) {
        //     $state.go(nextState, {rating_id: rating.id});
        // });
    }


    this.setSegment = function(segmentId) {
        this.savedSegment = segmentId;
    }

    this.getSegment = function() {
        return this.savedSegment;
    }

    this.nextSaveRatingEngine = function(nextState) {
        var changed = false,
            opts = RatingsEngineStore.settings,
            segment = RatingsEngineStore.getSegment();

        RatingsEngineStore.saveRating().then(function(rating) {
            $state.go(nextState, {rating_id: rating.id});
        });
    }



    this.setRating = function(rating) {
        this.currentRating = rating;
    }

    this.getRating = function(id) {
        var deferred = $q.defer();

        if(this.rating) {
            deferred.resolve(this.rating)
        } else {
            RatingsEngineService.getRating(id).then(function(data) {
                RatingsEngineStore.setRating(data);
                deferred.resolve(data);
            });
        }

        return deferred.promise;
    }

    this.getCurrentRating = function() {
        return this.currentRating;
    }

    this.saveRating = function(opts) {
        var deferred = $q.defer(),
            opts = opts || {},
            ClientSession = BrowserStorageUtility.getClientSession();

        opts.createdBy = opts.createdBy || ClientSession.EmailAddress;
        opts.type = opts.type || 'RULE_BASED',
        opts.displayName = 'testing making new engine';
        opts.segment = {'name': RatingsEngineStore.getSegment() };
        RatingsEngineService.saveRating(opts).then(function(data){
            deferred.resolve(data);
            RatingsEngineStore.setRating(data);
        });

        return deferred.promise;
    }
    
    this.getRatings = function() {
        var deferred = $q.defer();

        RatingsEngineService.getRatings().then(function(data) {
            RatingsEngineStore.ratings = data;
            deferred.resolve(data);
        });

        return deferred.promise;
    }

    this.getSegmentsCounts = function(segmentIds){
        var deferred = $q.defer();
        
        RatingsEngineService.getSegmentsCounts(segmentIds).then(function(response){
            deferred.resolve(response);
        });

        return deferred.promise;
    };

    this.getRatingsChartData = function(arrayofIds){
        var deferred = $q.defer();
        
        RatingsEngineService.getRatingsChartData(arrayofIds).then(function(response){
            deferred.resolve(response);
        });

        return deferred.promise;
    };


    this.setType = function(type) {
        this.type = type;
    }

    this.getType = function() {
        return this.type;
    }

})
.service('RatingsEngineService', function($q, $http, $state) {
    this.getRatings = function() {
        var deferred = $q.defer(),
            result,
            url = '/pls/ratingengines';

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);

            }, 
            function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';

                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.getSegmentsCounts = function(segmentIds) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/coverage',
            data: {
                segmentIds: segmentIds
            }
        }).then(function(response) {
            deferred.resolve(response.data);
        });

        return deferred.promise;
    }

    this.getRatingsChartData = function(arrayofIds) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/coverage',
            data: {
                ratingEngineIds: arrayofIds
            }
        }).then(function(response) {
            deferred.resolve(response.data);
        });

        return deferred.promise;
    }

    this.deleteRating = function(ratingName) {
        var deferred = $q.defer(),
            result,
            url = '/pls/ratingengines/' + ratingName;

        $http({
            method: 'DELETE',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);

            }, 
            function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.saveRating = function(opts) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines',
            data: opts
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    }

    this.getRating = function(id) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/ratingsengines/' + id
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    }


});
