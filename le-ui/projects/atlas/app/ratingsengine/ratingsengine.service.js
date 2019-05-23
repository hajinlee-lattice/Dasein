angular.module('lp.ratingsengine')
.service('RatingsEngineStore', function(
    $q, $state, $stateParams,  $rootScope, $timeout, $filter,
    RatingsEngineService, DataCloudStore, BrowserStorageUtility, SegmentStore, ImportWizardService, JobsStore, Banner
){
    var RatingsEngineStore = this;
    
    this.current = {
        ratings: [],
        tileStates: {},
        bucketCountMap: {}
    };

    this.ratingsSet = false;

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
            model: false,
            mapping: false
        }

        this.segment_form = {
            segment_selection: ''
        }

        this.currentRating = {};
        this.remodelIteration = null;
        this.iterationEnrichments = [];
        this.iterations = null;
        this.usedBy = null;
        this.rule = null;
        this.rating = null;
        this.rating_id;
        this.coverage = {};
        this.savedSegment = "";
        this.productsSelected = {};
        this.modelingStrategy = null;
        this.predictionType = 'PROPENSITY';
        this.type = null;
        this.configFilters = {};
        this.trainingSegment = null;
        this.trainingProducts = null;
        this.modelTrainingOptions = {
            "deduplicationType": "ONELEADPERDOMAIN",
            "excludePublicDomains": false,
            "transformationGroup": null
        },
        this.customEventModelingType = "";
        this.FieldDocument = {};
        this.fileName = "";
        this.applicationId = "";
        this.displayFileName = "";
        this.availableFields = [];

        this.wizardProgressItems = {
            "rulesprospects": [
                { 
                    label: 'Segment', 
                    state: 'segment', 
                    progressDisabled: true,
                    showNextSpinner: true,
                    nextLabel: 'Next, Choose Attributes', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRatingEngine(nextState);
                    }
                },{ 
                    label: 'Attributes', 
                    state: 'segment.attributes', 
                    progressDisabled: true,
                    nextLabel: 'Next, Set Rules',
                    nextFn: function(nextState){
                        var current = RatingsEngineStore.getRule();
                        if(current && current.rule){
                            SegmentStore.sanitizeRuleBuckets(current.rule, true)
                        }
                        
                        $state.go(nextState);
                    }
                },{ 
                    hide: true,
                    label: 'Add',
                    state: 'segment.attributes.add', 
                    progressDisabled: true,
                    nextLabel: 'Next, Rules',
                    nextFn: function(nextState){
                        var current = RatingsEngineStore.getRule();
                        if(current){
                            SegmentStore.sanitizeRuleBuckets( current.rule, true)
                        }
                        $state.go(nextState);
                    }
                },{ 
                    hide: true,
                    hideBack: true,
                    label: 'Picker',
                    state: 'segment.attributes.rules.picker', 
                    progressDisabled: true,
                    nextLabel: 'Back to Rules',
                    nextFn: function(nextState){
                        var current = RatingsEngineStore.getRule();
                        if(current){
                            SegmentStore.sanitizeRuleBuckets( current.rule, true)
                        }
                        $state.go(nextState);
                    }
                },{ 
                    hideBack: true,
                    label: 'Rules', 
                    state: 'segment.attributes.rules', 
                    progressDisabled: true,
                    nextLabel: 'Model', 
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRules(nextState);
                    } 
                },{
                    label: 'Summary', 
                    state: 'segment.attributes.rules.summary', 
                    progressDisabled: true,
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
                    progressDisabled: true,
                    showNextSpinner: true,
                    nextLabel: 'Next',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveRatingEngineAI(nextState);
                    } 
                },
                { 
                    label: 'Products', 
                    state: 'segment.products', 
                    progressDisabled: true,
                    nextLabel: 'Next',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveAIRatingModel(nextState);
                    } 
                },
                { 
                    label: 'Prioritization', 
                    state: 'segment.products.prioritization',
                    progressDisabled: true,
                    nextLabel: 'Next',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveAIRatingModel(nextState);
                    } 
                },
                { 
                    label: 'Training', 
                    state: 'segment.products.prioritization.training', 
                    progressDisabled: true,
                    nextLabel: 'Model',
                    showNextSpinner: true,
                    afterNextValidation: true,
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveAIRatingModel(nextState, 'training'); // validate here
                    } 
                },
                { 
                    label: 'Creation', 
                    state: 'segment.products.prioritization.training.creation', 
                    progressDisabled: true,
                    hideBack: true,
                    secondaryLinkLabel: 'Go to Model List',
                    secondaryLink: 'home.ratingsengine',
                    lastRoute: true,
                    nextLabel: 'Create another Model',
                    nextFn: function(nextState) {
                        $state.go('home.ratingsengine.ratingsenginetype');
                    } 
                }
            ],
            "customevent": [
                {
                    label:'Segment',
                    state:'segment',
                    progressDisabled: true,
                    showNextSpinner: true,
                    nextLabel: 'Next',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveCustomEventRatingEngine(nextState);
                    }
                },{
                    label:'Training',
                    state:'segment.training',
                    progressDisabled: true,
                    showNextSpinner: true,
                    nextLabel: 'Next',
                    nextFn: function(nextState) {
                        RatingsEngineStore.nextSaveCustomEventRatingModel(nextState);
                    }
                },{
                    label:'Field Mapping',
                    state:'segment.training.mapping',
                    progressDisabled: true,
                    nextLabel: 'Model',
                    nextFn: function(nextState) {
                        RatingsEngineStore.saveFieldMapping(nextState);
                    }
                },{
                    label:'Model Creation',
                    state:'segment.training.mapping.creation',
                    progressDisabled: true,
                    hideBack: true,
                    secondaryLinkLabel: 'Go to Model List',
                    secondaryLink: 'home.ratingsengine',
                    lastRoute: true,
                    nextLabel: 'Create another Model',
                    nextFn: function() {
                        $state.go('home.ratingsengine.ratingsenginetype', {}, {reload: true});
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

    this.getRemodelIteration = function() {
        return this.remodelIteration;
    }
    this.setRemodelIteration = function(remodelIteration) {
        this.remodelIteration = remodelIteration;
    }

    this.getIterationEnrichments = function() {
        return this.iterationEnrichments;
    }
    this.setIterationEnrichments = function(iterationEnrichments) {
        this.iterationEnrichments = iterationEnrichments;
    }

    this.getIterations = function() {
        return this.iterations;
    }
    this.setIterations = function(iterations) {
        this.iterations = iterations;
    }

    this.getUsedBy = function() {
        return this.usedBy;
    }
    this.setUsedBy = function(usedBy) {
        this.usedBy = usedBy;
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
        let currentRating = RatingsEngineStore.getCurrentRating();
        if(currentRating && segment && segment != null){
            currentRating.segment = segment;
        }
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
        var currentRating = RatingsEngineStore.getCurrentRating(),
            currentSegment = RatingsEngineStore.getSegment();
        
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
                    rule: SegmentStore.sanitizeRuleBuckets(angular.copy(current.rule), true) 
                }
            };

        RatingsEngineService.saveRules(opts).then(function(rating) {
            if(nextState) {
                $state.go(nextState, { rating_id: $stateParams.rating_id });
            } else {
                //console.log('!!!!!!!!!!!!!!!!!',rating);
            }
        });
    }

    this.nextSaveSummary = function(nextState){
        var rating = RatingsEngineStore.getCurrentRating();

        RatingsEngineStore.saveRating(rating).then(function(result) {
            $state.go(nextState, { rating_id: rating.id });
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
            RatingsEngineService.getRating(id).then(function(engine) {
                RatingsEngineStore.setRating(engine);
                deferred.resolve(engine);
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

    this.saveRating = function(rating) {
        var deferred = $q.defer(),
            ClientSession = BrowserStorageUtility.getClientSession(); 

        var opts = {
            createdBy: rating.createdBy !== undefined ? rating.createdBy : ClientSession.EmailAddress,
            type: rating.type !== undefined ? rating.type : 'RULE_BASED',
            segment: rating.segment || RatingsEngineStore.getSegment(),
            displayName: $stateParams.opts !== undefined ? $stateParams.opts.displayName : rating.displayName,
            status: $stateParams.opts !== undefined ? $stateParams.opts.status : rating.status,
            id: rating.id,
            note: $stateParams.opts !== undefined ? $stateParams.opts.note : rating.note
        };

        var params = {};
        if (rating.type !== undefined && rating.type === 'CROSS_SELL') {
            opts.advancedRatingConfig = {
                cross_sell: {
                    modelingStrategy: rating.advancedRatingConfig.cross_sell.modelingStrategy
                }
            };
        } else if (rating.type != undefined && rating.type == 'CUSTOM_EVENT') {
            if ($stateParams.rating_id && RatingsEngineStore.getCustomEventModelingType() == 'LPI') {
                params = {
                    'unlink-segment': true
                }
            }
        }

        RatingsEngineService.saveRating(opts, params).then(function(data){
            RatingsEngineStore.setRating(data);
            deferred.resolve(data);
        });

        return deferred.promise;
    }


    this.saveIteration = function(stateToValidate) {

        var deferred = $q.defer(),
            ratingEngine = RatingsEngineStore.getRatingEngine(),
            engineId = ratingEngine.id,
            iteration = RatingsEngineStore.getRemodelIteration(),
            clientSession = BrowserStorageUtility.getClientSession(),
            createdBy = clientSession.EmailAddress;

        iteration.AI.derived_from_rating_model = iteration.AI.id;
        iteration.AI.createdBy = createdBy;

        if(iteration.AI.advancedModelingConfig.cross_sell){
            iteration.AI.trainingSegment = RatingsEngineStore.getTrainingSegment();
            iteration.AI.advancedModelingConfig.cross_sell.filters = RatingsEngineStore.configFilters;// RatingsEngineStore.getConfigFilters();
        } else {
            iteration.AI.advancedModelingConfig.custom_event = RatingsEngineStore.configFilters;//RatingsEngineStore.getConfigFilters();
        }

        // Sanitize iteration to remove data
        delete iteration.AI.pid;
        delete iteration.AI.id;
        delete iteration.AI.modelingJobId;
        delete iteration.AI.modelingJobStatus;
        delete iteration.AI.modelSummaryId;

        // Save iteration
        RatingsEngineService.saveIteration(engineId, iteration).then(function(result){

            var modelId = result.AI.id;

            RatingsEngineStore.setValidation(stateToValidate, false);
            // Validate Model
            RatingsEngineService.validateModel(engineId, modelId, RatingsEngineStore.getRatingEngine()).then(function(result) {
                var success = result.data == true;
                if (success) {

                    var enrichments = RatingsEngineStore.getIterationEnrichments();

                    console.log(enrichments);

                    RatingsEngineService.launchModeling(engineId, modelId, enrichments).then(function(applicationId){

                        RatingsEngineStore.setApplicationId(applicationId);
                        JobsStore.inProgressModelJobs[engineId] = null;

                        var result = {
                            applicationId: applicationId,
                            showProgress: false
                        }
                        deferred.resolve(result);
                    });

                } else {
                    var errorResult = {
                        result: result,
                        showProgress: false
                    }
                    deferred.resolve(errorResult);
                    RatingsEngineStore.setValidation(stateToValidate, !success);
                }
            });
        });

        return deferred.promise;

    };

    
    this.setRatings = function(ratings, ignoreGetCoverage) {
        
        this.current.ratings = ratings;
        RatingsEngineStore.ratingsSet = true;

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

        this.ratingsSet = false;

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

    this.getScorableAccounts = function(ratingEngine, engineId, iterationId) {
        var deferred = $q.defer();

        RatingsEngineService.getScorableAccounts(ratingEngine, engineId, iterationId).then(function(result) {
            deferred.resolve(result);
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
        var rule = SegmentStore.sanitizeRuleBuckets(angular.copy(CurrentRatingsEngine.rule));

        CoverageMap = CoverageMap || {};

        CoverageMap.restrictNotNullSalesforceId = false;
        CoverageMap.segmentIdModelRules = [{
            segmentId: segmentId,
            ratingRule: {
                bucketToRuleMap: rule.ratingRule.bucketToRuleMap,
                defaultBucketName: rule.ratingRule.defaultBucketName
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
        return this.rating_id;
    }

    this.setRatingId = function() {
        this.rating_id = $stateParams.rating_id;
    }

    this.getRatingEngine = function() {
        return this.rating;
    }
    this.setRatingEngine = function(rating) {
        this.rating = rating;   
    }

    this.getProductsSelectedIds = function() {
        var ids = Object.keys(this.productsSelected);
        return ids;
    }

    this.deleteRating = function(ratingId) {
        var deferred = $q.defer();

        RatingsEngineService.deleteRating(ratingId).then(function(result) {
            if (result == true) {
                // immediately remove deleted rating from ratinglist
                RatingsEngineStore.setRatings(RatingsEngineStore.current.ratings.filter(function(rating) { 
                    return rating.id != ratingId;
                }), true);
            }
            deferred.resolve(result);
        });



        return deferred.promise;
    }

    this.getModelTrainingOptions = function() {
        return this.modelTrainingOptions;
    }

    this.setModelTrainingOptions = function(trainingOptions) {
        this.modelTrainingOptions = trainingOptions;
    }

    this.setCustomEventModelingType = function(customEventModelingType) {
        this.customEventModelingType = customEventModelingType;
    }

    this.getCustomEventModelingType = function() {
        return this.customEventModelingType;
    }

    this.setModelingStrategy = function(modelingStrategy) {
        this.modelingStrategy = modelingStrategy;
    }
    this.getModelingStrategy = function() {
        return this.modelingStrategy;
    }
    this.setPredictionType = function(predictionType) {
        this.predictionType = predictionType;
    }
    this.getPredictionType = function() {
        return this.predictionType;
    }

    this.setFieldDocument = function(fieldDocument) {
        RatingsEngineStore.FieldDocument = fieldDocument;
    }

    this.getFieldDocument = function() {
        return RatingsEngineStore.FieldDocument;
    }

    this.setCSVFileName = function(fileName) {
        RatingsEngineStore.fileName = fileName;
    }

    this.getCSVFileName = function() {
        return RatingsEngineStore.fileName;
    }

    this.setDisplayFileName = function(fileName) {
        RatingsEngineStore.displayFileName = fileName;
    }

    this.getDisplayFileName = function() {
        return RatingsEngineStore.displayFileName;
    }
    
    this.setApplicationId = function(appId) {
        RatingsEngineStore.applicationId = appId;
    }

    this.getApplicationId = function() {
        return RatingsEngineStore.applicationId;
    }

    this.setConfigFilters = function(configFilters) {
        this.configFilters = configFilters;
    }
    this.getConfigFilters = function() {
        return this.configFilters;
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

    this.nextSaveCustomEventRatingEngine = function(nextState){

        var opts = {
          type: "CUSTOM_EVENT",
          id: $stateParams.rating_id ? $stateParams.rating_id : null
        };

        RatingsEngineStore.saveRating(opts).then(function(rating) {
            $state.go(nextState, { rating_id: rating.id });
        });
    }

    this.nextSaveCustomEventRatingModel = function(nextState) {
        var ratingId = $stateParams.rating_id;
        RatingsEngineStore.getRating(ratingId).then(function(rating) {

            var model = rating.latest_iteration,
                predictionType = RatingsEngineStore.getPredictionType(),
                customEventModelingType = RatingsEngineStore.getCustomEventModelingType(),
                dataStores = customEventModelingType == "LPI" ? ["CustomFileAttributes", "DataCloud"] : ["CDL", "DataCloud"],
                modelTrainingOptions = RatingsEngineStore.getModelTrainingOptions(),
                fileName = RatingsEngineStore.getCSVFileName(),
                displayFileName = RatingsEngineStore.getDisplayFileName(),
                obj = {};

            obj = {
                AI: {
                    id: model.AI.id,
                    predictionType: predictionType,
                    advancedModelingConfig: {
                        'custom_event': {
                            customEventModelingType: customEventModelingType,
                            dataStores: dataStores,
                            sourceFileName: fileName,
                            sourceFileDisplayName: displayFileName,
                            deduplicationType: modelTrainingOptions['deduplicationType'],
                            excludePublicDomains: modelTrainingOptions['excludePublicDomains'],
                            transformationGroup: modelTrainingOptions['transformationGroup']
                        }
                    }
                }
            }

            RatingsEngineService.updateRatingModel(ratingId, obj.AI.id, obj).then(function(model) {
                $state.go(nextState, { rating_id: ratingId });
            });
        });
    }

    this.isUnmappedField = function(fieldMapping) {
        return !fieldMapping.mappedToLatticeField || this.availableFields.indexOf(fieldMapping.userField) >= 0;
    }

    this.saveFieldMapping = function(nextState) {

        var ratingId = $stateParams.rating_id;        
        var FieldDocument = RatingsEngineStore.getFieldDocument();

        FieldDocument.fieldMappings.forEach(function(fieldMapping) {
            if (fieldMapping.ignored) {
                FieldDocument.ignoredFields.push(fieldMapping.userField);
                delete fieldMapping.ignored;
            } else if (RatingsEngineStore.isUnmappedField(fieldMapping)) {

                var customEventModelingType = RatingsEngineStore.getCustomEventModelingType(),
                    dataStores = customEventModelingType == "LPI" ? ["CustomFileAttributes", "DataCloud"] : ["CDL", "DataCloud"];

                if (dataStores.indexOf('CustomFileAttributes') < 0) {
                    fieldMapping.mappedToLatticeField = false;
                    fieldMapping.mappedField = fieldMapping.userField;
                    fieldMapping.ignored = true;
                    FieldDocument.ignoredFields.push(fieldMapping.mappedField);
                }
            }
        });


        ImportWizardService.SaveFieldDocuments(RatingsEngineStore.getCSVFileName(), FieldDocument, {excludeCustomFileAttributes: RatingsEngineStore.getCustomEventModelingType() === 'CDL'}, true).then(function(result) {
                RatingsEngineStore.getRating(ratingId).then(function(rating) {
                    RatingsEngineStore.nextLaunchAIModel(nextState, rating.latest_iteration);
                });
        });
    }

    this.nextSaveRatingEngineAI = function(nextState){
        var ratingId = $stateParams.rating_id;
        var engineType = RatingsEngineStore.getModelingStrategy(),
            opts =  {
                type: "CROSS_SELL",
                advancedRatingConfig: {
                    cross_sell: {
                        modelingStrategy: engineType
                    }
                }
            };
        if(ratingId){
            opts.id = ratingId;
        }

        RatingsEngineStore.saveRating(opts).then(function(rating) {
            $state.go(nextState, { rating_id: rating.id });
        });
    }

    /**
     * [nextSaveAIRatingModel 
     * @param  {[string]} nextState [the next state]
     * @param  {[sting]} validate  [what you want to setValidation for, not a boolean]
     */
    this.nextSaveAIRatingModel = function(nextState, validate){
        var ratingId = $stateParams.rating_id;

        RatingsEngineStore.getRating(ratingId).then(function(rating){

            var model = rating.latest_iteration.AI,
                targetProducts = (model.targetProducts === []) ? [] : RatingsEngineStore.getProductsSelectedIds(),
                predictionType = RatingsEngineStore.getPredictionType(),
                configFilters = RatingsEngineStore.getConfigFilters(),
                modelingStrategy = $stateParams.engineType,
                trainingSegment = RatingsEngineStore.getTrainingSegment(),
                trainingProducts = RatingsEngineStore.getTrainingProducts(),
                obj = {};

            obj = {
                AI: {
                    id: model.id,
                    predictionType: predictionType,
                    trainingSegment: trainingSegment,
                    advancedModelingConfig: {
                        cross_sell: {
                            targetProducts: targetProducts,
                            trainingProducts: trainingProducts,
                            modelingStrategy: modelingStrategy,
                            filters: configFilters
                        }
                    }
                }
            };

            RatingsEngineService.updateRatingModel(ratingId, obj.AI.id, obj).then(function(model) {

                var route = nextState,
                    lastRoute = route.split(/[\.]+/);

                if(validate) {
                    rating.latest_iteration = model;
                    RatingsEngineStore.setValidation(validate, false);
                    RatingsEngineService.validateModel(ratingId, obj.AI.id, rating).then(function(result) {
                        var success = result.data == true;
                        if (success) {
                            if (lastRoute[lastRoute.length-1] === 'creation') {
                                // console.log("Model Updated & Launch", model);
                                RatingsEngineStore.nextLaunchAIModel(nextState, model);
                            } else {
                                // console.log("Model Updated", model);
                                if(nextState) {
                                    $state.go(nextState, { rating_id: ratingId });
                                }
                            }
                        }
                        RatingsEngineStore.setValidation(validate, !success);
                    });
                } else {
                    if (lastRoute[lastRoute.length-1] === 'creation') {
                        // console.log("Model Updated & Launch", model);
                        RatingsEngineStore.nextLaunchAIModel(nextState, model);
                    } else {
                        // console.log("Model Updated", model);
                        if(nextState) {
                            $state.go(nextState, { rating_id: ratingId });
                        }
                    }
                }

            });
        });
    }

    this.setAvailableFields = function(availableFields) {
        this.availableFields = availableFields;
    }

    this.getAvailableFields = function() {
        return this.availableFields;
    }
    
    this.getRatingModel = function(engineId, modelId) {
        var deferred = $q.defer();

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
            RatingsEngineStore.setApplicationId(applicationid);
            JobsStore.inProgressModelJobs[currentRating.id] = null;

            var id = applicationid;
            // console.log('Model Launched', id, nextState);
            if(nextState) {
                $state.go(nextState, { ai_model_job_id: id });
            }
        });
    }

    this.formatTrainingAttributes = function(type) {
        switch (type) {
            case 'DataCloud':
                return 'Lattice Data Cloud';
            case 'CDL':
                return 'Lattice Database';
            case 'CustomFileAttributes':
                return 'Training File';
        }
    }

    this.getModel = function(ratingId){
        var deferred = $q.defer();
        RatingsEngineStore.getRating(ratingId).then(function(engine){
            RatingsEngineStore.setRating(engine);
            if(engine.latest_iteration.AI){
                RatingsEngineStore.getRatingModel(ratingId, engine.latest_iteration.AI.id).then(function(model){
                    deferred.resolve(model);
                });
            }else {
                deferred.resolve(engine.latest_iteration);
            }
        });   
        return deferred.promise;  
    };

    this.saveRatingStatus = function(rating_id, status, action){

        var deferred = $q.defer();
        var newRating = {
            id: rating_id,
            status: status
        };
        RatingsEngineService.saveRating(newRating, {}, action).then(function(data){
            deferred.resolve({success: true});
        });
        return deferred.promise;
    };

    this.getProductCoverage = function(ratingEngine, productsList, purchasedBeforePeriod) {
        var deferred = $q.defer();

        var productIds = productsList.map(function(product) {
            return product.ProductId;
        });

        RatingsEngineService.getProductCoverage(ratingEngine, productIds, purchasedBeforePeriod).then(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;

    }

    this.getIterationFromDashboard = function(ratingEngineDashboard, iterationId) {

        // console.log(ratingEngineDashboard.iterations);

        let dashboardIterations = ratingEngineDashboard.iterations;
        let validIterations = dashboardIterations.filter(iteration => iteration.modelingJobStatus == 'Completed');        
        let filterValid = $filter('filter')(validIterations, {id:iterationId});

        console.log(filterValid);

        if (validIterations.length == 0) {
            return null;
        } else if (filterValid.length == 0) {
            let lastValidIndex = validIterations.length-1;
            return validIterations[lastValidIndex];
        } else {
            return filterValid[0];
        }

        // var iterations = $filter('filter')(ratingEngineDashboard.iterations, {id:iterationId});

        // console.log(iterations);

        // return iterations && iterations.length != 1 ? null : iterations[0];
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
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }
                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.getScorableAccounts = function(ratingEngine, engineId, iterationId) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/' + engineId + '/ratingmodels/' + iterationId + '/modelingquery/count',
            data: ratingEngine,
            params: {
                querytype: 'TARGET'
            },
            cache: true
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
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
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }
                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.getRatingsChartData = function(CoverageRequest) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/coverage',
            data: CoverageRequest,
            cache: true
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

    this.saveRating = function(opts, params, action) {
        var deferred = $q.defer(),
            createAction = action ? action : 'true';

        $http({
            method: 'POST',
            url: '/pls/ratingengines?create-action=' + createAction,
            data: opts,
            params: params || {}
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

    this.saveRules = function(opts) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/' + opts.rating_id + '/ratingmodels/' + opts.model_id,
            data: opts.model
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

    this.getRating = function(id) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/ratingengines/' + id
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

    this.getRatingModels = function(id) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/ratingengines/' + id + '/ratingmodels'
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

    this.getRatingDashboard = function(id) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/ratingengines/' + id + '/dashboard'
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

    this.createAIModel = function(ratingid, modelid){
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/ratingengines/' + ratingid + '/ratingmodels/' + modelid + '/model',
            headers: {
                'Accept': 'text/plain'
            }
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

    this.updateRatingModel = function(ratingid, modelid, opts){
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url:  '/pls/ratingengines/'+ratingid+'/ratingmodels/'+modelid,
            data: opts
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
    this.getRatingModel = function(ratingId, modelId){
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url:  '/pls/ratingengines/' + ratingId + '/ratingmodels/' + modelId
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
    this.getTrainingCounts = function(ratingId, modelId, ratingEngine, queryType){
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url:  '/pls/ratingengines/' + ratingId + '/ratingmodels/' + modelId + '/modelingquery/count',
            params: {
                querytype: queryType
            },
            data: ratingEngine
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

    this.validateModel = function(ratingId, modelId, ratingEngine){
        var deferred = $q.defer(),
            method = ratingEngine ? 'POST' : 'GET';

        $http({
            method: method,
            url: '/pls/ratingengines/' + ratingId + '/ratingmodels/' + modelId + '/model/validate',
            headers: {
                'Accept': 'application/json'
            },
            data: ratingEngine
        }).then(
            function onSuccess(response) {
                deferred.resolve(response);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }
                deferred.resolve(response);
            }
        );
    
        return deferred.promise;
    }
    
    this.GetRatingEnginesDependenciesModelView = function(id, errorDisplayCallback) {
        var deferred = $q.defer(),
            result,
            url = '/pls/ratingengines/' + id + '/dependencies/modelAndView';

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json',
                'ErrorDisplayCallback': errorDisplayCallback
            }
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

        this.getProductCoverage = function(ratingEngine, productIds, purchasedBeforePeriod){
            var deferred = $q.defer();

            $http({
                method: 'POST',
                url: '/pls/ratingengines/coverage/segment/products',
                data: {
                    ratingEngine: ratingEngine,
                    productIds: productIds
                },
                params: purchasedBeforePeriod ? {'purchasedbeforeperiod': purchasedBeforePeriod} : {}
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
