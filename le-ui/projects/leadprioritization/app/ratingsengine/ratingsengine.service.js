angular.module('lp.ratingsengine')
.service('RatingsEngineStore', function(
    $q, $state, $stateParams, RatingsEngineService, DataCloudStore, RatingsEngineAIStore,
    BrowserStorageUtility, SegmentStore, $timeout
){
    var RatingsEngineStore = this;
    
    this.current = {
        ratings: [],
        tileStates: {},
        bucketCountMap: {}
    };

    this.init = function() {
        this.settings = {};

        this.validation = {
            segment: true,
            attributes: true,
            rules: true,
            summary: true,
            prospect: false,
            products: false,
            refine: false,
            model: false
        }

        this.segment_form = {
            segment_selection: ''
        }

        this.currentRating = {};
        this.rule = null;
        this.rating = null;
        this.rating_id
        this.type = null;
        this.coverage = {};
        this.savedSegment = "";

        this.wizardProgressItems = {
            "all": [
                { 
                    label: 'Segment', 
                    state: 'segment', 
                    nextLabel: 'Next, Choose Attributes', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRatingEngine(nextState);
                    } 
                },{ 
                    label: 'Attributes', 
                    state: 'segment.attributes', 
                    nextLabel: 'Next, Set Rules'
                },{ 
                    label: 'Rules', 
                    state: 'segment.attributes.rules', 
                    nextLabel: 'Next, Summary', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRules(nextState);
                    } 
                },{ 
                    label: 'Summary', 
                    state: 'segment.attributes.rules.summary', 
                    nextLabel: 'Save', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveSummary(nextState);
                    }
                }
            ],
            "ai": [
                { 
                    label: 'Segment', 
                    state: 'segment', 
                    nextLabel: 'Next', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRatingEngineAI(nextState);
                    } 
                },
                { 
                    label: 'Prospect', 
                    state: 'segment.prospect', 
                    nextLabel: 'Next, choose Products', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveTypeModel(nextState);
                    } 
                },
                { 
                    label: 'Products', 
                    state: 'segment.prospect.products', 
                    nextLabel: 'Next, choose what to model',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveProductToAIModel(nextState);
                    } 
                },
                { 
                    label: 'Refine', 
                    state: 'segment.prospect.products.refine',
                    nextLabel: 'Create Model',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRefineToAIModel(nextState);
                    } 
                },
                { 
                    label: 'Model', 
                    state: 'segment.prospect.products.refine.model', 
                    nextLabel: 'Rating Engines'
                    // nextFn: function(nextState) {
                    //     RatingsEngineStore.nextSaveRatingEngine(nextState);
                    // } 
                }
            ],
            "segment": [
                { 
                    label: 'Segment', 
                    state: 'segment', 
                    nextLabel: 'Back To Dashboard', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRatingEngine(nextState);
                    } 
                }
            ],
            "attributes": [
                { 
                    label: 'Attributes', 
                    state: 'segment.attributes', 
                    nextLabel: 'Next, Set Rules'
                },{ 
                    label: 'Rules', 
                    state: 'segment.attributes.rules', 
                    nextLabel: 'Back To Dashboard', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRules(nextState);
                    }
                }
            ],
            "rules": [
                { 
                    label: 'Attributes', 
                    state: 'segment.attributes', 
                    nextLabel: 'Next, Set Rules'
                },{ 
                    label: 'Rules', 
                    state: 'segment.attributes.rules', 
                    nextLabel: 'Back To Dashboard', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRules(nextState);
                    }
                }
            ],
            "summary": [
                { 
                    label: 'Summary', 
                    state: 'segment.attributes.rules.summary', 
                    nextLabel: 'Back To Dashboard', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveSummary(nextState);
                    }
                }
            ]
        };
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

    this.getWizardProgressItems = function(step) {
        return this.wizardProgressItems[(step || 'all')];
    }

    this.setSettings = function(obj) {
        var obj = obj || {};

        for (var i in obj) {
            var key = i,
                value = obj[i];
            this.settings[key] = value;
        }
    }

    this.setSegment = function(segment) {
        //console.log("set segment service", segment);
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

    this.nextSaveGeneric = function(nextState) {
        var changed = false,
            opts = RatingsEngineStore.settings;
        
        $state.go(nextState, { rating_id: $stateParams.rating_id });
    }

    this.nextSaveRatingEngine = function(nextState) {
        var currentRating = RatingsEngineStore.getCurrentRating();

        console.log("save engine", currentRating);
        RatingsEngineStore.saveRating(currentRating).then(function(rating) {
            $state.go(nextState, { rating_id: rating.id });
        });
    }

    this.nextSaveRules = function(nextState) {
        var current = RatingsEngineStore.getRule(),
            opts = {
                rating_id: $stateParams.rating_id,
                model_id: current.rule.id,
                model: { 
                    rule: SegmentStore.sanitizeRuleBuckets(angular.copy(current.rule)) 
                }
            };

        RatingsEngineService.saveRules(opts).then(function(rating) {
            $state.go(nextState, { rating_id: $stateParams.rating_id });
        });
    }

    this.nextSaveSummary = function(nextState){
        RatingsEngineStore.saveRating().then(function(result) {
            $state.go(nextState, { rating_id: $stateParams.rating_id });
        });
    }

    this.setRule = function(rule) {
        this.rule = rule;
    }

    this.getRule = function(rule) {
        return this.rule;
    }

    this.hasRules = function(rating) {
        try {
            if (Object.keys(rating.coverage).length) {
                return true;
            } else {
                return false;
            }
        } catch(err) {
            return false;
        }
    }

    this.setRating = function(rating) {
        this.currentRating = rating;
    }

    this.getRating = function(id) {
        var deferred = $q.defer();

        if (this.currentRating === {}) {
            deferred.resolve(this.currentRating);
        } else {
            RatingsEngineService.getRating(id).then(function(data) {
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
            rating = RatingsEngineStore.getCurrentRating();

        opts = {
            createdBy: opts.createdBy || ClientSession.EmailAddress,
            type: opts.type || 'RULE_BASED',
            segment: rating.segment || RatingsEngineStore.getSegment(),
            displayName: rating.displayName,
            status: rating.status,
            id: rating.id,
            note: rating.note
        };

        RatingsEngineService.saveRating(opts).then(function(data){
            RatingsEngineStore.setRating(data);
            deferred.resolve(data);
        });

        return deferred.promise;
    }
    
    this.setRatings = function(ratings, ignoreGetCoverage) {
        
        this.current.ratings = ratings;

        if (!ignoreGetCoverage) {
            var ids = [];
            RatingsEngineStore.current.tileStates = {};
            angular.forEach(ratings, function(rating) {
                var id = rating.id;
                ids.push(id);
                RatingsEngineStore.current.tileStates[id] = {
                    showCustomMenu: false,
                    editRating: false,
                    saveEnabled: false
                };
            });

            RatingsEngineStore.getChartDataConcurrently(ids.reverse());
        }
    }
    
    this.getRatings = function(active, cacheOnly) {
        var deferred = $q.defer();

        if (this.current.ratings.length > 0) {
            deferred.resolve(this.current.ratings);

            if (cacheOnly) {
                return this.current;
            }
        }

        RatingsEngineService.getRatings(active).then(function(data) {
            RatingsEngineStore.setRatings(data);
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

    this.getChartDataConcurrently = function(all){
        var deferred = $q.defer();
        var chunks = [], size = Math.max(1, Math.floor(all.length / 5));

        while (all.length > 0) {
            chunks.push(all.splice(0, size));
        }

        var incrementor = 0;
        var maxConcurrent = chunks.length;
        var bucketCountMap = {};
        var rating = null;
        
        $timeout(function() {
            chunks.forEach(function(ids) {
                RatingsEngineService.getRatingsChartData({
                    ratingEngineIds: ids
                }).then(function(response) {
                    incrementor++;

                    Object.keys(response.ratingEngineIdCoverageMap).reverse().forEach(function(key) {
                        rating = response.ratingEngineIdCoverageMap[key];
                        RatingsEngineStore.current.bucketCountMap[key] = rating;
                    })

                    if (incrementor >= maxConcurrent) {
                        deferred.resolve(bucketCountMap);
                    }
                });
            });
        }, 100);

        return deferred.promise;
    };

    this.getRatingsChartData = function(all){
        var deferred = $q.defer();
        
        RatingsEngineService.getRatingsChartData({
            ratingEngineIds: all
        }).then(function(response){
            //RatingsEngineStore.setCoverage(response);
            deferred.resolve(response);
        });

        return deferred.promise;
    };

    this.getCoverageMap = function(CurrentRatingsEngine, segmentId){
        var deferred = $q.defer();

        SegmentStore.sanitizeRuleBuckets(CurrentRatingsEngine.rule, true);

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
            //RatingsEngineStore.setCoverage(response);
            deferred.resolve(response);
        });

        return deferred.promise;
    };

    this.getBucketRuleCounts = function(restrictions, segmentId){
        var deferred = $q.defer();

        var buckets = restrictions.map(function(bucket, index) {
            var label = bucket.bucketRestriction.attr,
                type = label.split('.')[0] == 'Contact' ? 'contact' : 'account',
                object = {
                    "segmentId": segmentId,
                    "responseKeyId": label + '_' +  index
                };

            object[type + '_restriction'] = SegmentStore.sanitizeSegmentRestriction([ bucket ])[0];

            return object;
        });

        var CoverageMap = {
            "restrictNotNullSalesforceId": false,
            "segmentIdAndSingleRules": buckets
        };

        RatingsEngineService.getRatingsChartData(CoverageMap).then(function(response){
            deferred.resolve(response);
        });

        return deferred.promise;
    };

    this.getCoverage = function() {
        return this.coverage;
    }

    this.setCoverage = function(bucketCountMap) {
        this.current.bucketCountMap = bucketCountMap;
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

    this.checkRatingsBuckets = function(map) {
        var buckets = ['A','A-','B','C','D','F'];
        var generated = this.generateRatingsBuckets();

        buckets.forEach(function(key, value) {
            var bucket = map[key];

            if (!bucket) {
                map[key] = generated[key];
            }
        })
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

    this.getRatingId = function() {
        return vm.rating_id;
    }

    this.setRatingId = function() {
        this.rating_id = $stateParams.rating_id;
    }

    this.deleteRating = function(ratingId) {
        var deferred = $q.defer();

        RatingsEngineService.deleteRating(ratingId).then(function(result) {
            deferred.resolve(result);
        });

        // immediately remove deleted rating from ratinglist
        this.setRatings(this.current.ratings.filter(function(rating) { 
            return rating.id != ratingId;
        }), true);

        return deferred.promise;
    }

    this.nextSaveRatingEngineAI = function(nextState){
        var currentRating = RatingsEngineStore.getCurrentRating();
        var opts =  {
            type: currentRating.type || 'AI_BASED',
           
        };
        console.log("save engine AI", opts);
        RatingsEngineStore.saveRating(opts).then(function(rating) {
            $state.go(nextState, { rating_id: currentRating.id });
        });
    }
    
    this.nextSaveTypeModel = function(nextState){
        var currentRating = RatingsEngineStore.getCurrentRating();
        var obj = currentRating.ratingModels[0].AI;
        obj.workflowType = 'CROSS_SELL';
        var opts = {
            AI: obj
        };
        RatingsEngineService.updateRatingModel(currentRating.id, obj.id, opts).then(function(model) {
            $state.go(nextState, { rating_id: model.id });
        });
    }

    this.nextSaveProductToAIModel = function(nextState){
        var currentRating = RatingsEngineStore.getCurrentRating();
        var productsIds = RatingsEngineAIStore.getProductsSelectedIds();
        var obj = currentRating.ratingModels[0].AI;
        obj.targetProducts  = productsIds;//'CROSS_SELL';
        var opts = {
            AI: obj
        };

        RatingsEngineService.updateRatingModel(currentRating.id, obj.id, opts).then(function(model) {
            $state.go(nextState, { rating_id: model.id });
        });
       

    }

    this.nextSaveRefineToAIModel = function(nextState) {
        var currentRating = RatingsEngineStore.getCurrentRating();
        var obj = currentRating.ratingModels[0].AI;
        obj.targetCustomerSet = "new";
        obj.modelingMethod = "PROPENSITY";
        var opts = {
            AI: obj
        };


        RatingsEngineService.updateRatingModel(currentRating.id, obj.id, opts).then(function(applicationid) {
            console.log(applicationid);
            RatingsEngineStore.nextLaunchAIModel(nextState);
            // RatingsEngineService.createAIModel(currentRating.id, obj.id, opts).then(function(applicationid) {
            //     var obj = {
            //         modelingJobId : applicationid
            //     }
            //     RatingsEngineService.updateRatingModel(currentRating.id, obj.id, opts).then(function(model) {
                    
            //     });
            // });
        });
    }
    

    this.nextLaunchAIModel = function(nextState){
        var currentRating = RatingsEngineStore.getCurrentRating();
        var obj = currentRating.ratingModels[0].AI;
        RatingsEngineStore.tmpId = obj.id;
        var opts = {};
        console.log('Launching the model');
        RatingsEngineService.createAIModel(currentRating.id, obj.id, opts).then(function(applicationid) {
            console.log('Application id', applicationid);
            var id = RatingsEngineStore.tmpId;
            var obj = {
                modelingJobId : applicationid
            }
            console.log('Model Launched', id, nextState);
            $state.go(nextState, { ai_model: id });
            // RatingsEngineService.updateRatingModel(currentRating.id, id, opts).then(function(model) {
            //     console.log('Job created', model);
            //     $state.go(nextState, { rating_id: model.id });
                
            // });
        });
    }

})
.service('RatingsEngineService', function($q, $http, $state) {
    this.getRatings = function(active) {
        var deferred = $q.defer(),
            result,
            url = '/pls/ratingengines' + (active ? '?status=ACTIVE' : '');

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
            },
            cache: true
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
            data: CoverageRequest,
            cache: true
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

    this.createAIModel = function(ratingid, modelid, opts){
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/models/rating/'+modelid,
            data: opts
        }).then(
            function(response){
                deferred.resolve(response.data);
            }
        );

        return deferred.promise;
    }
    this.updateRatingModel = function(ratingid, modelid, opts){
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url:  '/pls/ratingengines/'+ratingid+'/ratingmodels/'+modelid,
            data: opts
        }).then(function(response){
            deferred.resolve(response.data);
        });
       

        return deferred.promise;
    }
});
