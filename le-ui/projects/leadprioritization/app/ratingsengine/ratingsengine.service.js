angular.module('lp.ratingsengine')
.service('RatingsEngineStore', function(
    $q, $state, $stateParams, RatingsEngineService, DataCloudStore, 
    BrowserStorageUtility, SegmentStore
){
    var RatingsEngineStore = this;

    this.init = function() {
        this.settings = {};

        this.validation = {
            segment: true,
            attributes: true,
            rules: true
        }

        this.segment_form = {
            segment_selection: ''
        }

        this.currentRating = {};
        this.rule = null;
        this.rating = null;
        this.ratings = null;
        this.type = null;
        this.coverage = {};
        this.savedSegment = "";
    }

    this.init();
    
    this.clear = function() {
        console.log('clear')
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


    this.setSegment = function(segment) {
        this.savedSegment = segment;
    }

    this.getSegment = function() {
        return this.savedSegment;
    }

    var getRatingsEngineRule = function(RatingsEngineModels) {
        var data = RatingsEngineModels[0],
            rule = (data && data.rule ? data.rule : {}),
            rule = rule || {};

        return rule;
    }

    this.nextSaveRatingEngine = function(nextState) {
        var changed = false,
            opts = RatingsEngineStore.settings,
            currentRating = RatingsEngineStore.getCurrentRating(),
            segment = RatingsEngineStore.getSegment();

        RatingsEngineStore.saveRating(currentRating).then(function(rating) {
            $state.go(nextState, {rating_id: rating.id});
        });
    }

    this.nextSaveRules = function(nextState) {
        var current = RatingsEngineStore.getRule();

        var opts = {
            rating_id: $stateParams.rating_id,
            model_id: current.rule.id,
            model: { 
                rule: SegmentStore.sanitizeRuleBuckets(current.rule) 
            }
        };

        console.log('nextSaveRules', opts, current);
        RatingsEngineService.saveRules(opts).then(function(rating) {
            $state.go(nextState, { rating_id: rating.id });
        });
    }

    this.setRule = function(rule) {
        this.rule = rule;
    }

    this.getRule = function(rule) {
        return this.rule;
    }

    this.setRating = function(rating) {
        this.currentRating = rating;
    }

    this.getRating = function(id) {
        var deferred = $q.defer();

        if (this.currentRating.id) {
            deferred.resolve(this.currentRating)
        } else {
            RatingsEngineService.getRating(id).then(function(data) {
                console.log('setRating', data);
                RatingsEngineStore.setRating(data);
                deferred.resolve(data);
            });
        }

        return deferred.promise;
    }

    this.getRatingDashboard = function(id) {
        var deferred = $q.defer();

        RatingsEngineService.getRatingDashboard(id).then(function(data) {
            deferred.resolve(data);
        });

        return deferred.promise;
    }

    this.getCurrentRating = function() {
        return this.currentRating;
    }

    this.saveRating = function(opts) {
        var deferred = $q.defer(),
            opts = opts || {},
            ClientSession = BrowserStorageUtility.getClientSession(),
            segment = RatingsEngineStore.getSegment();

        opts.createdBy = opts.createdBy || ClientSession.EmailAddress;
        opts.type = opts.type || 'RULE_BASED',
        opts.displayName = 'testing making new engine';
        opts.segment = {'name': segment.name };
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
        
        RatingsEngineService.getRatingsChartData({
                ratingEngineIds: arrayofIds
        }).then(function(response){
            RatingsEngineStore.setCoverage(response);
            deferred.resolve(response);
        });

        return deferred.promise;
    };

    this.getCoverageMap = function(CurrentRatingsEngine, segmentId){
        var deferred = $q.defer();
        var CoverageMap = {
            "restrictNotNullSalesforceId":false,
            "segmentIdModelRules": [{
                "segmentId": segmentId,
                "ratingRule": {
                    "bucketToRuleMap": CurrentRatingsEngine.rule.ratingRule.bucketToRuleMap,
                    "defaultBucketName": CurrentRatingsEngine.rule.ratingRule.defaultBucketName
                }
            }]
        };

        RatingsEngineService.getRatingsChartData(CoverageMap).then(function(response){
            RatingsEngineStore.setCoverage(response);
            deferred.resolve(response);
        });

        return deferred.promise;
    };

    this.getCoverage = function() {
        return this.coverage;
    }

    this.setCoverage = function(coverage) {
        this.coverage = coverage;
    }

    // this.getRatingsCounts = function(Ratings, noSalesForceId) {
    //     var deferred = $q.defer(),
    //         ratings_ids = [],
    //         noSalesForceId = noSalesForceId || false;
    //     if(Ratings && typeof Ratings === 'object') {
    //         Ratings.forEach(function(value, key) {
    //             ratings_ids.push(value.id);
    //         });
    //         RatingsEngineService.getRatingsCounts(ratings_ids, noSalesForceId).then(function(data) {
    //             deferred.resolve(data);
    //         });
    //     }
    //     return deferred.promise;
    // }

    this.setType = function(type) {
        this.type = type;
    }

    this.getType = function() {
        return this.type;
    }

    this.generateRatingsBuckets = function() {
        var restriction = {
            logicalRestriction: {
                operator: "AND",
                restrictions: []
            }
        };

        var template = {
            account_restriction: angular.copy(restriction),
            contact_restriction: angular.copy(restriction)
        }

        return {
            "A":  angular.copy(template),
            "A-": angular.copy(template),
            "B":  angular.copy(template),
            "C":  angular.copy(template),
            "D":  angular.copy(template),
            "F":  angular.copy(template)
        };
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

    this.getRatingsChartData = function(CoverageRequest) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/coverage',
            data: CoverageRequest
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

    this.saveRules = function(opts) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/' + opts.rating_id + '/ratingmodels/' + opts.model_id,
            data: opts.model
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    }

    this.getRating = function(id) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/ratingengines/' + id
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    }

    this.getRatingDashboard = function(id) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/ratingengines/' + id + '/dashboard'
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    }
});
