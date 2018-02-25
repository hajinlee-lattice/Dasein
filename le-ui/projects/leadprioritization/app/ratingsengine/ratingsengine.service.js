angular.module('lp.ratingsengine')
.service('RatingsEngineStore', function(
    $q, $state, $stateParams,  $rootScope, RatingsEngineService, DataCloudStore,
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
            add: true,
            picker: true,
            rules: true,
            summary: true,
            prospect: false,
            products: false,
            prioritization: false,
            training: true,
            refine: false,
            model: false
        }

        this.segment_form = {
            segment_selection: ''
        }

        this.currentRating = {};
        this.rule = null;
        this.rating = null;
        this.rating_id;
        this.coverage = {};
        this.savedSegment = "";
        this.productsSelected = {};
        this.predictionType = 'PROPENSITY';
        this.type = null;
        this.modelingConfigFilters = null;
        this.trainingSegment = null;
        this.trainingProducts = null;

        this.wizardProgressItems = {
            "rulesprospects": [
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
                    hide: true,
                    label: 'Add',
                    state: 'segment.attributes.add', 
                    nextLabel: 'Next, Rules'
                },{ 
                    hide: true,
                    hideBack: true,
                    label: 'Picker',
                    state: 'segment.attributes.rules.picker', 
                    nextLabel: 'Back to Rules'
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
            "editrules": [
                { 
                    hide: true,
                    hideBack: true,
                    label: 'Add',
                    state: 'segment.attributes.add', 
                    nextLabel: 'Back to Rules'
                },{ 
                    hide: true,
                    hideBack: true,
                    label: 'Picker',
                    state: 'segment.attributes.rules.picker', 
                    nextLabel: 'Back to Rules'
                },{
                    label: 'Rules', 
                    hideBack: true,
                    state: 'segment.attributes.rules', 
                    nextLabel: 'Back To Dashboard', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRules(nextState);
                    }
                }
            ],
            "productpurchase": [
                { 
                    label: 'Segment', 
                    state: 'segment', 
                    nextLabel: 'Next',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRatingEngineAI(nextState);
                    } 
                },
                { 
                    label: 'Products', 
                    state: 'segment.products', 
                    nextLabel: 'Next',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveAIRatingModel(nextState);
                    } 
                },
                { 
                    label: 'Prioritization', 
                    state: 'segment.products.prioritization',
                    nextLabel: 'Next',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveAIRatingModel(nextState);
                    } 
                },
                { 
                    label: 'Training', 
                    state: 'segment.products.prioritization.training', 
                    nextLabel: 'Next',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveAIRatingModel(nextState);
                    } 
                },
                { 
                    label: 'Creation', 
                    state: 'segment.products.prioritization.training.creation', 
                    hideBack: true,
                    secondaryLinkLabel: 'Go to Rating Engine List',
                    secondaryLink: 'home.ratingsengine',
                    lastRoute: true,
                    nextLabel: 'Create another Rating Engine',
                    nextFn: function(nextState) {
                        $state.go('home.ratingsengine.ratingsenginetype');
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
        return this.wizardProgressItems[(step || 'rulesprospects')];
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
        var currentSegment = RatingsEngineStore.getSegment();

        console.log("save engine", currentRating);
        if (currentRating.segment != null && currentSegment != null && currentRating.segment.name != currentSegment.name) {
            RatingsEngineStore.setRating({});
        }
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

                var coverage = [];
                for(var bucket in rating.coverage) {
                    coverage.push({bucket: bucket, count: rating.coverage[bucket]})
                }

                RatingsEngineStore.current.bucketCountMap[id] = 
                { 
                    accountCount: rating.accountsInSegment,
                    contactCount: rating.contactsInSegment,
                    bucketCoverageCounts: coverage
                };
            });
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

    this.getCoverageMap = function(CurrentRatingsEngine, segmentId, CoverageMap){
        var deferred = $q.defer();
        var CoverageMap = CoverageMap || {};

        SegmentStore.sanitizeRuleBuckets(CurrentRatingsEngine.rule, true);

        CoverageMap.restrictNotNullSalesforceId = false;
        CoverageMap.segmentIdModelRules = [{
            segmentId: segmentId,
            ratingRule: {
                bucketToRuleMap: CurrentRatingsEngine.rule.ratingRule.bucketToRuleMap,
                defaultBucketName: CurrentRatingsEngine.rule.ratingRule.defaultBucketName
            }
        }];

        RatingsEngineService.getRatingsChartData(CoverageMap).then(function(response){
            deferred.resolve(response);
        });

        return deferred.promise;
    };


    this.getBucketRuleCounts = function(restrictions, segmentId){
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

        return {
            "restrictNotNullSalesforceId": false,
            "segmentIdAndSingleRules": buckets
        };
    };

    this.getCoverage = function() {
        return this.coverage;
    }

    this.setCoverage = function(bucketCountMap) {
        this.current.bucketCountMap = bucketCountMap;
    }
    this.setType = function(type, engineType) {
        this.type = {
            wizardType: type,
            engineType: engineType
        }
    }
    this.getType = function() {
        return this.type;
    }

    this.clearSelection = function () {
        this.productsSelected = {};
    }
    this.getProducts = function (params) {
        var deferred = $q.defer();
        RatingsEngineService.getProducts(params).then(function (data) {
            deferred.resolve(data.data);
        });
        return deferred.promise;
    }
    this.selectProduct = function (id, name) {
        if (this.productsSelected[id]) {
            delete this.productsSelected[id];
        } else {
            this.productsSelected[id] = name;
        }
    }
    this.selectAllProducts = function(allProducts) {
        allProducts.forEach(function(product){
            if(product.Selected === false || product.Selected === undefined){
                RatingsEngineStore.selectProduct (product.ProductId, product.ProductName);
            }
            product.Selected = true;
        });
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

    this.checkRatingsBuckets = function(map) {
        var buckets = ['A','B','C','D','E','F'];
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
            "A": angular.copy(template),
            "B": angular.copy(template),
            "C": angular.copy(template),
            "D": angular.copy(template),
            "E": angular.copy(template),
            "F": angular.copy(template)
        };
    }

    this.getRatingId = function() {
        return vm.rating_id;
    }

    this.setRatingId = function() {
        this.rating_id = $stateParams.rating_id;
    }

    this.getProductsSelectedIds = function() {
        var ids = Object.keys(this.productsSelected);
        return ids;
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

    this.setPredictionType = function(predictionType) {
        this.predictionType = predictionType;
    }
    this.getPredictionType = function() {
        return this.predictionType;
    }

    
    this.setModelingConfigFilters = function(modelingConfigFilters) {
        var currentConfig = this.modelingConfigFilters;
        if(currentConfig != null){
            this.modelingConfigFilters = angular.extend(currentConfig, modelingConfigFilters);
        } else {
            this.modelingConfigFilters = modelingConfigFilters;
        }
    }
    this.getModelingConfigFilters = function() {
        return this.modelingConfigFilters;
    }

    this.setTrainingSegment = function(trainingSegment) {
        this.trainingSegment = trainingSegment;
    }
    this.getTrainingSegment = function() {
        return this.trainingSegment;
    }

    this.setTrainingProducts = function(trainingProducts) {
        this.trainingProducts = trainingProducts;
    }
    this.getTrainingProducts = function() {
        return this.trainingProducts;
    }

    this.nextSaveRatingEngineAI = function(nextState){

        var engineType = $stateParams.engineType,
            opts =  {
                type: "AI_BASED",
                activeModel: {
                    AI: {
                        workflowType: 'CROSS_SELL',
                        modelingStrategy: engineType
                    }
                }
            };

        RatingsEngineStore.saveRating(opts).then(function(rating) {
            $state.go(nextState, { rating_id: rating.id });
        });
    }

    this.nextSaveAIRatingModel = function(nextState){

        var ratingId = $stateParams.rating_id;
        RatingsEngineStore.getRating(ratingId).then(function(rating){

            var model = rating.activeModel.AI,
                targetProducts = (model.targetProducts === []) ? [] : RatingsEngineStore.getProductsSelectedIds(),
                predictionType = RatingsEngineStore.getPredictionType(),
                modelingConfigFilters = (model.modelingConfigFilters === null) ? RatingsEngineStore.getModelingConfigFilters() : null,
                modelingStrategy = $stateParams.engineType,
                trainingSegment = RatingsEngineStore.getTrainingSegment(),
                trainingProducts = RatingsEngineStore.getTrainingProducts(),
                obj = {};

            obj = {
                AI: {
                    id: rating.activeModel.AI.id,
                    targetProducts: targetProducts,
                    predictionType: predictionType,
                    modelingStrategy: modelingStrategy,
                    modelingConfigFilters: modelingConfigFilters,
                    trainingSegment: trainingSegment,
                    trainingProducts: trainingProducts
                }
            };

            // console.log("GET RATING & MODEL CREATE OBJECT", obj.AI);

            RatingsEngineService.updateRatingModel(ratingId, obj.AI.id, obj).then(function(model) {

                // console.log("MODEL", model.AI);

                var route = nextState,
                    lastRoute = route.split(/[\.]+/);

                if (lastRoute[lastRoute.length-1] === 'creation') {
                    // console.log("Model Updated & Launch", model);
                    RatingsEngineStore.nextLaunchAIModel(nextState, model);
                } else {
                    // console.log("Model Updated", model);
                    if(nextState) {
                        $state.go(nextState, { rating_id: ratingId });
                    }
                }
            });
        });
        
    }
    
    this.getRatingModel = function(engineId, modelId) {
        var deferred = $q.defer();

        // console.log(engineId, modelId);
        RatingsEngineService.getRatingModel(engineId, modelId).then(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    this.getTrainingCounts = function(engineId, modelId, ratingEngine, queryType) {
        var deferred = $q.defer();

        RatingsEngineService.getTrainingCounts(engineId, modelId, ratingEngine, queryType).then(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    this.nextLaunchAIModel = function(nextState, model){
        var currentRating = RatingsEngineStore.getCurrentRating(),
            obj = model.AI;
        
        RatingsEngineStore.tmpId = obj.id;

        // console.log('Launching the model', obj);
        RatingsEngineService.createAIModel(currentRating.id, obj.id).then(function(applicationid) {
            var id = applicationid.Result;
            // console.log('Model Launched', id, nextState);
            $state.go(nextState, { ai_model_job_id: id });
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

    this.getProducts = function (params) {
        var deferred = $q.defer(),
            max = params.max,
            offset = params.offset,
            data = [],
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

    this.createAIModel = function(ratingid, modelid){
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/' + ratingid + '/ratingmodels/' + modelid + '/model',
            headers: {
                'Accept': 'text/plain'
            }
        }).then(function(response){
            deferred.resolve(response.data);
        });

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
    this.getRatingModel = function(ratingId, modelId){
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url:  '/pls/ratingengines/' + ratingId + '/ratingmodels/' + modelId
        }).then(function(response){
            deferred.resolve(response.data);
        });
    
        return deferred.promise;
    }

    this.getTrainingCounts = function(ratingId, modelId, ratingEngine, queryType){
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url:  '/pls/ratingengines/' + ratingId + '/ratingmodels/' + modelId + '/modelingquery/count',
            params: {
                querytype: queryType
            },
            data: ratingEngine
        }).then(function(response){
            deferred.resolve(response.data);
        });
    
        return deferred.promise;
    }
});
