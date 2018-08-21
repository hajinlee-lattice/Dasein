angular.module('lp.ratingsengine.dashboard', [
    'mainApp.appCommon.directives.barchart'
])
.controller('RatingsEngineDashboard', function(
    $q, $stateParams, $state, $rootScope, $scope, $sce,
    RatingsEngineStore, RatingsEngineService, AtlasRemodelStore, Modal,
    Dashboard, RatingEngine, Model, Notice, IsRatingEngine, IsPmml, Products, AuthorizationUtility, FeatureFlagService
) {
    var vm = this,
        flags = FeatureFlagService.Flags();

    var featureFlagsConfig = {};
    featureFlagsConfig[flags.PLAYBOOK_MODULE] = true;

    angular.extend(vm, {
        playbookEnabled: AuthorizationUtility.checkFeatureFlags(featureFlagsConfig),
        deactivateInProgress: false,
        dashboard: Dashboard,
        ratingEngine: RatingEngine,
        modelSummary: Model,
        products: Products,
        barChartConfig: {
            'data': {
                'tosort': true,
                'sortBy': 'bucket_name',
                'trim': true,
                'top': 6,
            },
            'chart': {
                'header':'Value',
                'emptymsg': '',
                'usecolor': true,
                'color': '#e8e8e8',
                'mousehover': false,
                'type': 'integer',
                'showstatcount': false,
                'maxVLines': 3,
                'showVLines': false,
            },
            'vlines': {
                'suffix': ''
            },
            'columns': [{
                'field': 'num_leads',
                'label': 'Records',
                'type': 'number',
                'chart': true,
            }]
        },
        barChartLiftConfig: {
            'data': {
                'tosort': true,
                'sortBy': 'bucket_name',
                'trim': true,
                'top': 6,
            },
            'chart': {
                'header':'Value',
                'emptymsg': '',
                'usecolor': true,
                'color': '#e8e8e8',
                'mousehover': false,
                'type': 'decimal',
                'showstatcount': false,
                'maxVLines': 3,
                'showVLines': true,
            },
            'vlines': {
                'suffix': 'x'
            },
            'columns': [{
                    'field': 'lift',
                    'label': 'Lift',
                    'type': 'string',
                    'suffix': 'x',
                    'chart': true
                }
            ]
        },
        modalConfig: {
            'name': "rating_engine_deactivate",
            'dischargeaction' :'cancel',
            'confirmaction' : 'ok',
        }
    });

    // console.log(vm.dashboard);


    vm.modalCallback = function (args) {
        var modal = Modal.get(vm.modalConfig.name);

        if('closedForced' === args.action) {
        }else if(vm.modalConfig.dischargeaction === args.action){
            Modal.modalRemoveFromDOM(modal, args);
        } else if(vm.modalConfig.confirmaction === args.action){
            modal.waiting(true);
            modal.disableDischargeButton(true);
            vm.deactivateRating().then(function(result){
                if(result.success === true) {
                    Modal.modalRemoveFromDOM(modal, args);
                }
            });
        
        }
    }

    vm.viewUrl = function () {
        return 'app/ratingsengine/content/dashboard/deactive-message.component.html';
    }


    vm.isActive = function(status) {
        return (status === 'ACTIVE' ? true : false);
    }
    
    vm.deactivate = function(){
        if(vm.dashboard.plays && vm.dashboard.plays.length > 0 && vm.ratingEngine.status === 'ACTIVE'){
            // vm.toggleModal();

            var plays = vm.generateHtmlFromPlays();
            Modal.warning({
                name: "rating_engine_deactivate",
                icon: 'ico ico-model ico-black',
                title: "Deactivate Model",
                message: $sce.trustAsHtml('<section class=rating-engine-deactivate style=margin-top:0><p>Deactivating a model will prevent all of the plays which use it from launching. This model is currently being used by the following plays:<ul class=plays-used-list>' + plays + '</ul></section>'),
                confirmtext: "Deactivate",
                confirmcolor: "blue-button",
                headerconfig: { "background-color":"white", "color":"black" },
            }, vm.modalCallback);
        }else{
            vm.deactivateRating();
        }
    }

    vm.generateHtmlFromPlays = function() {
        var html = "";
        vm.dashboard.plays.forEach(function(play) {
            html += "<li>" + play.displayName + "</li>"
        });
        return html;
    }

    vm.disableScoringButton = function(){
        if(vm.ratingEngine.status === 'INACTIVE' || vm.deactivateInProgress === true){
            return true;
        }

        if(!vm.isRulesBased){
            return vm.dashboard.summary.bucketMetadata ? false : true ;
        }else{
            return false;
        }
    }

    vm.deactivateRating = function(){
        vm.deactivateInProgress = true;
        var deferred = $q.defer();

        var newStatus = (vm.isActive(vm.ratingEngine.status) ? 'INACTIVE' : 'ACTIVE'),
            newRating = {
                id: vm.ratingEngine.id,
                status: newStatus
            }
        var msgStatus = newStatus == 'ACTIVE' ? 'activated' : 'deactivated';    
        var model = vm.ratingEngine.scoring_iteration;
        RatingsEngineService.saveRating(newRating).then(function(data){

            //This call is made because the POST API does not return 
            // The activeModel. Next release M-21 the json structure is going to change
            RatingsEngineService.getRating(vm.ratingEngine.id).then(
                function(dataUpdated){
                    vm.ratingEngine = dataUpdated;
                    $rootScope.$broadcast('statusChange', { 
                        activeStatus: data.status
                    });
                    Notice.success({
                        delay: 3000,
                        title: 'Deactivate Scoring', 
                        message: 'Your scoring has been '+msgStatus+'.'
                    });
                    RatingsEngineService.getRatingDashboard(newRating.id).then(function(data){
                        vm.dashboard.plays = data.plays;
                        vm.initDataModel();
                        deferred.resolve({success: true});
                        vm.deactivateInProgress = false;
                    });
                }
            );
            
        });
        return deferred.promise;
    }

    vm.status_toggle = vm.isActive(vm.ratingEngine.status);

    vm.toggleActive = function() {
        var active = vm.isActive(vm.ratingEngine.status);
        if(active && vm.dashboard.plays.length > 0){
            var modal = Modal.get(vm.modalConfig.name);
            modal.toggle();
        } else {
            var newStatus = (vm.isActive(vm.ratingEngine.status) ? 'INACTIVE' : 'ACTIVE'),
                newRating = {
                id: vm.ratingEngine.id,
                status: newStatus
            }
            RatingsEngineService.saveRating(newRating).then(function(data){
                $rootScope.$broadcast('statusChange', { 
                    activeStatus: data.status
                });
                vm.ratingEngine = data;
                vm.status_toggle = vm.isActive(data.status);
                vm.toggleScoringButtonText = (vm.status_toggle ? 'Deactivate Scoring' : 'Activate Scoring');
            });
        }
    }

    vm.initDataModel = function(){
        vm.relatedItems = [];
        Object.keys(vm.dashboard.dependencies).forEach(function(type) {
            if (vm.dashboard.dependencies[type]) {
                vm.dashboard.dependencies[type].forEach(function(name) {
                    vm.relatedItems.push({
                        type: type,
                        name: name
                    });
                });
            }
        });

        vm.hasBuckets = vm.ratingEngine.counts != null;
        vm.statusIsActive = (vm.ratingEngine.status === 'ACTIVE');
        vm.isRulesBased = (vm.ratingEngine.type === 'RULE_BASED');
        vm.isPublished = vm.dashboard.summary.isPublished ? true : false;
        
        RatingsEngineStore.setRatingEngine(vm.ratingEngine);

        if(vm.ratingEngine.type === 'CROSS_SELL' || vm.ratingEngine.type === 'CUSTOM_EVENT') {
            vm.ratingEngine.chartConfig = vm.barChartLiftConfig;
            vm.publishOrActivateButtonLabel = 'New Scoring Configuration';//vm.dashboard.summary.bucketMetadata.length > 0 ? 'New Scoring Configuration' : 'Activate Scoring';
        } else {
            vm.ratingEngine.chartConfig = vm.barChartConfig;
        }        


        if (vm.isRulesBased) {
            vm.toggleScoringButtonText = (vm.status_toggle ? 'Deactivate Scoring' : 'Activate Scoring');
            vm.modelingStrategy = 'RULE_BASED';
        } else {
            if(vm.ratingEngine.published_iteration || vm.ratingEngine.scoring_iteration) {
                vm.model = vm.ratingEngine.published_iteration ? vm.ratingEngine.published_iteration.AI : vm.ratingEngine.scoring_iteration.AI;
            } else {
                vm.model = vm.ratingEngine.latest_iteration.AI;
            }
            var type = vm.ratingEngine.type.toLowerCase();

            if (type === 'cross_sell') {
                if (typeof vm.model.advancedModelingConfig[type].filters != 'undefined' && (Object.keys(vm.model.advancedModelingConfig[type].filters).length === 0 || (vm.model.advancedModelingConfig[type].filters['PURCHASED_BEFORE_PERIOD'] && Object.keys(vm.model.advancedModelingConfig[type].filters).length === 1)) && vm.model.trainingSegment == null && vm.model.advancedModelingConfig[type].filters.targetProducts == null) {
                    vm.hasSettingsInfo = false;
                } else {
                    vm.hasSettingsInfo = true;
                }

                vm.targetProducts = vm.model.advancedModelingConfig[type].targetProducts;
                vm.modelingStrategy = vm.model.advancedModelingConfig[type].modelingStrategy;
                vm.configFilters = vm.model.advancedModelingConfig[type].filters;
                vm.trainingProducts = vm.model.advancedModelingConfig[type].trainingProducts;

                if (vm.configFilters && vm.configFilters['SPEND_IN_PERIOD']) {
                    if (vm.configFilters['SPEND_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL') {
                        vm.spendCriteria = 'at least';
                    } else {
                        vm.spendCriteria = 'at most';
                    }
                }

                if (vm.configFilters && vm.configFilters['QUANTITY_IN_PERIOD']) {
                    if (vm.configFilters['QUANTITY_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL') {
                        vm.quantityCriteria = 'at least';
                    } else {
                        vm.quantityCriteria = 'at most';
                    }
                }

                if (vm.targetProducts && vm.targetProducts.length != 0) {
                    vm.targetProductName = vm.returnProductNameFromId(vm.targetProducts[0]);
                }
                if (vm.trainingProducts && vm.trainingProducts.length != 0) {
                    vm.trainingProductName = vm.returnProductNameFromId(vm.trainingProducts[0]);
                }

                if (vm.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
                    vm.ratingEngineType = 'First Purchase Cross-Sell'
                } else if (vm.modelingStrategy === 'CROSS_SELL_REPEAT_PURCHASE') {
                    vm.ratingEngineType = 'Repeat Purchase Cross-Sell'
                }
            } else {

                console.log(vm.model);

                vm.modelingStrategy = 'CUSTOM_EVENT';
                vm.ratingEngineType = 'Custom Event';
            }

            vm.modelSummary = vm.model.modelSummaryId;
            vm.predictionType = vm.model.predictionType;
            vm.trainingSegment = vm.model.trainingSegment;

            if (vm.predictionType === 'PROPENSITY') {
                vm.prioritizeBy = 'Likely to Buy';
            } else if (vm.predictionType === 'EXPECTED_VALUE') {
                vm.prioritizeBy = 'Likely Amount of Spend';
            }
        }
    }

    vm.init = function() {

        console.log(vm.ratingEngine);
        // console.log(vm.dashboard);

        vm.initDataModel();
    }

    vm.isIterationActive = function(iterationId){
        if(vm.ratingEngine.scoring_iteration != null) {
            if(vm.ratingEngine.scoring_iteration.AI.id == iterationId){
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    vm.returnProductNameFromId = function(productId) {
        var products = vm.products,
            product = products.find(function(obj) { return obj.ProductId === productId.toString() });

        return product.ProductName;
    };

    vm.getCustomEventAvailableAttributes = function(model) {
        var dataStore = model.advancedModelingConfig.custom_event.dataStores;
        return dataStore.length == 1 ? RatingsEngineStore.formatTrainingAttributes(dataStore[0]) : 
                RatingsEngineStore.formatTrainingAttributes(dataStore[0]) + ' + ' + RatingsEngineStore.formatTrainingAttributes(dataStore[1]);
    };
    vm.getScoringButtonLable = function(){
        if(vm.isRulesBased){
            return vm.ratingEngine.status === 'INACTIVE' ? 'Activate Scoring' : 'Deactivate Scoring' ; 
        }else {
            return 'Deactivate Scoring';
        }
    };
    vm.disableButtonScoring = function(){
        if(!vm.isRulesBased){
            // return vm.dashboard.summary.bucketMetadata ? false : true ;
            var deactivate = (vm.ratingEngine.status === 'INACTIVE' || vm.deactivateInProgress === true);
            return deactivate;
        }else{
            return vm.deactivateInProgress; 
        }
    };

    vm.isJobRunning = function(){
        var jobStatus = '';
        if(vm.ratingEngine.type === 'RULE_BASED'){
            var activeModel = vm.ratingEngine.activeModel;
            jobStatus = activeModel.rule.modelingJobStatus;
        }else{
            var model = vm.ratingEngine.scoring_iteration ? vm.ratingEngine.scoring_iteration : vm.ratingEngine.latest_iteration;
            jobStatus = model.AI.modelingJobStatus;
        }

        switch(jobStatus){
            case 'Completed':
                return false;

            default: return true;
        }

    };

    vm.canNewScoringConfig = function() {
        var can = !((vm.dashboard.summary.bucketMetadata && vm.dashboard.summary.bucketMetadata.length === 0) || vm.isJobRunning());
        if(vm.dashboard.iterations){
            var iterations  = vm.dashboard.iterations;
            can = iterations.some(function(iteration){
                return (iteration.modelingJobStatus === 'Completed');
            });
        }
        return can;
    }

    vm.canCreatePlay = function(){
        if(vm.isRulesBased && vm.playbookEnabled){
            return vm.ratingEngine.status === 'ACTIVE';
        }
        if(!vm.isRulesBased && vm.playbookEnabled){
            // console.log(vm.dashboard.summary.isPublished);
            return (vm.dashboard.summary.isPublished && vm.ratingEngine.status === 'ACTIVE');
        }
    };

    vm.remodel = function(iteration){
        var engineId = vm.ratingEngine.id,
            modelId = iteration.id;

        RatingsEngineStore.getRatingModel(engineId, modelId).then(function(result){
            AtlasRemodelStore.setRemodelIteration(result);
            RatingsEngineStore.setRatingEngine(vm.ratingEngine);
            $state.go('home.ratingsengine.remodel', { engineId: engineId, modelId: modelId });
        });
    }

    vm.viewIteration = function(iteration){
        var modelId = iteration.modelSummaryId,
            rating_id = $stateParams.rating_id;

        $state.go('home.model.attributes', { 
            rating_id: rating_id, 
            modelId: modelId,
            viewingIteration: true
        },{ reload:true });   

    }

    $scope.$on("$destroy", function() {
        var modal = Modal.get(vm.modalConfig.name);
        if (modal) {
            Modal.modalRemoveFromDOM(modal, {name: 'rating_engine_deactivate'});
        }
    });

    vm.init();
});
