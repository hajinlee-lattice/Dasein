angular.module('lp.ratingsengine.dashboard', [
    'mainApp.appCommon.directives.barchart'
])
.controller('RatingsEngineDashboard', function(
    $q, $stateParams, $state, $rootScope, $scope, $sce,
    RatingsEngineStore, RatingsEngineService, AtlasRemodelStore, Modal,
    Dashboard, RatingEngine, Model, Notice, IsRatingEngine, IsPmml, Products, TargetProducts, TrainingProducts, AuthorizationUtility, FeatureFlagService, DataCollectionStatus
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
        targetProducts: TargetProducts,
        trainingProducts: TrainingProducts,
        periodType: DataCollectionStatus.ApsRollingPeriod,
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
                message: $sce.trustAsHtml('<section class=rating-engine-deactivate style=margin-top:0><p>Deactivating a model will prevent all of the campaigns which use it from launching. This model is currently being used by the following campaigns:<ul class=plays-used-list>' + plays + '</ul></section>'),
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

    vm.init = function() {
        vm.relatedItems = [];
        Object.keys(vm.dashboard.dependencies).forEach(function(type) {
            if (vm.dashboard.dependencies[type]) {
                vm.dashboard.dependencies[type].forEach(function(name) {
                    type = type == 'Play' ? 'Campaign' : type;
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
        vm.isPublishedOrScored = (vm.ratingEngine.published_iteration || vm.ratingEngine.scoring_iteration) ? true : false;
        
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
            if(vm.isPublishedOrScored) {
                vm.model = vm.ratingEngine.published_iteration ? vm.ratingEngine.published_iteration.AI : vm.ratingEngine.scoring_iteration.AI;
                vm.modelSummary = vm.model.modelSummaryId;
            } else {
                vm.model = vm.ratingEngine.latest_iteration.AI;

                var dashboardIterations = vm.dashboard.iterations;
                vm.activeIterations = [];
                angular.forEach(dashboardIterations, function(iteration){
                    if (iteration.modelSummaryId && iteration.modelingJobStatus == "Completed") {
                        vm.activeIterations.push(iteration);
                    }
                });
                vm.modelSummary = vm.activeIterations.length > 0 ? vm.activeIterations[vm.activeIterations.length - 1].modelSummaryId : null;
                
            }
            var type = vm.ratingEngine.type.toLowerCase();

            if (type === 'cross_sell') {

                if(Array.isArray(vm.targetProducts)){
                    vm.targetProductsIsArray = true;
                    vm.tooltipContent = angular.copy(vm.targetProducts);
                    vm.targetProducts = vm.targetProducts.length + ' selected';
                } else {
                    vm.targetProductsIsArray = false;
                    vm.targetProducts = vm.targetProducts.ProductName;
                }

                vm.modelingStrategy = vm.model.advancedModelingConfig[type].modelingStrategy;
                vm.configFilters = vm.model.advancedModelingConfig[type].filters;

                vm.hasSettingsInfo = 
                    ((vm.trainingProducts || vm.trainingSegment) ||
                    vm.modelingStrategy == 'CROSS_SELL_REPEAT_PURCHASE' && Object.keys(vm.model.advancedModelingConfig[type].filters).length > 1 ||
                    vm.modelingStrategy == 'CROSS_SELL_FIRST_PURCHASE' && Object.keys(vm.model.advancedModelingConfig[type].filters).length != 0)
                    ? true : false;

                vm.oneTrainingProduct = typeof vm.trainingProducts == 'object' ? true : false;

                if (vm.configFilters && vm.configFilters['SPEND_IN_PERIOD']) {
                    vm.spendCriteria = vm.configFilters['SPEND_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL' ? 'at least' : 'at most';
                }

                if (vm.configFilters && vm.configFilters['QUANTITY_IN_PERIOD']) {
                    vm.quantityCriteria = vm.configFilters['QUANTITY_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL' ? 'at least' : 'at most';
                }

                if (vm.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
                    vm.ratingEngineType = 'First Purchase Cross-Sell'
                } else if (vm.modelingStrategy === 'CROSS_SELL_REPEAT_PURCHASE') {
                    vm.ratingEngineType = 'Repeat Purchase Cross-Sell'
                }
            } else {
                vm.modelingStrategy = 'CUSTOM_EVENT';
                vm.ratingEngineType = 'Custom Event';
            }

            vm.predictionType = vm.model.predictionType;
            vm.trainingSegment = vm.model.trainingSegment;

            if (vm.predictionType === 'PROPENSITY') {
                vm.prioritizeBy = 'Likely to Buy';
            } else if (vm.predictionType === 'EXPECTED_VALUE') {
                vm.prioritizeBy = 'Likely Amount of Spend';
            }
        }
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

    vm.getCustomEventAvailableAttributes = function(model) {
        var dataStore = model.advancedModelingConfig.custom_event.dataStores;
        if(dataStore){
            return dataStore.length == 1 ? RatingsEngineStore.formatTrainingAttributes(dataStore[0]) : 
                RatingsEngineStore.formatTrainingAttributes(dataStore[0]) + ' + ' + RatingsEngineStore.formatTrainingAttributes(dataStore[1]);
        } else {
            return false;
        }

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
            var latest_iteration = vm.ratingEngine.latest_iteration;
            jobStatus = latest_iteration.rule.modelingJobStatus;
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
        if(vm.playbookEnabled){
            return (vm.dashboard.summary.isPublished && vm.ratingEngine.status === 'ACTIVE');
        }else {
            return false;
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

    vm.getPeriodType = function(value) {
        return value > 1 ? vm.periodType + 's' : vm.periodType;
    }

    $scope.$on("$destroy", function() {
        var modal = Modal.get(vm.modalConfig.name);
        if (modal) {
            Modal.modalRemoveFromDOM(modal, {name: 'rating_engine_deactivate'});
        }
    });

    vm.init();
});
