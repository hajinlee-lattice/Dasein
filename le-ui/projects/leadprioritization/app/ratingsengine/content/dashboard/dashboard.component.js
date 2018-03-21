angular.module('lp.ratingsengine.dashboard', [
    'mainApp.appCommon.directives.barchart'
])
.controller('RatingsEngineDashboard', function(
    $q, $stateParams, $state, $rootScope, $scope, 
    RatingsEngineStore, RatingsEngineService,  ModalStore,
    Dashboard, RatingEngine, Model, IsRatingEngine, IsPmml, Products
) {
    var vm = this;

    angular.extend(vm, {
        dashboard: Dashboard,
        ratingEngine: RatingEngine,
        products: Products
    });

    vm.initModalWindow = function() {
        vm.config = {
            'name': "rating_engine_deactivate",
            'type': 'sm',
            'title': 'Deactivate Model',
            'titlelength': 100,
            'dischargetext': 'CANCEL',
            'dischargeaction' :'cancel',
            'confirmtext': 'DEACTIVATE',
            'confirmaction' : 'deactivate',
            'icon': 'ico ico-model ico-black',
            'showclose': false
        };
    
        vm.modalCallback = function (args) {
            if('closedForced' === args) {
            }else if(vm.config.dischargeaction === args){
                vm.toggleModal();
            } else if(vm.config.confirmaction === args){
                var modal = ModalStore.get(vm.config.name);
                modal.waiting(true);
                modal.disableDischargeButton(true);
                vm.deactivateRating();
            }
        }
        vm.toggleModal = function () {
            var modal = ModalStore.get(vm.config.name);
            if(modal){
                modal.toggle();
            }
        }
        vm.viewUrl = function () {
            return 'app/ratingsengine/content/dashboard/deactive-message.component.html';
        }

        $scope.$on("$destroy", function() {
            ModalStore.remove(vm.config.name);
        });
    }

    vm.isActive = function(status) {
        return (status === 'ACTIVE' ? true : false);
    }

    vm.deactivateRating = function(){
        var newStatus = (vm.isActive(vm.ratingEngine.status) ? 'INACTIVE' : 'ACTIVE'),
        newRating = {
            id: vm.ratingEngine.id,
            status: newStatus
        }
        RatingsEngineService.saveRating(newRating).then(function(data){
            vm.ratingEngine = data;
            vm.status_toggle = vm.isActive(data.status);
            vm.toggleScoringButtonText = (vm.status_toggle ? 'Deactivate Scoring' : 'Activate Scoring');
            vm.toggleModal();
        });
    }

    vm.status_toggle = vm.isActive(vm.ratingEngine.status);

    vm.toggleActive = function() {
        var active = vm.isActive(vm.ratingEngine.status);
        if(active && vm.dashboard.plays.length > 0){
            var modal = ModalStore.get(vm.config.name);
            modal.toggle();
        } else {
            var newStatus = (vm.isActive(vm.ratingEngine.status) ? 'INACTIVE' : 'ACTIVE'),
                newRating = {
                id: vm.ratingEngine.id,
                status: newStatus
            }
            RatingsEngineService.saveRating(newRating).then(function(data){
                vm.ratingEngine = data;
                vm.status_toggle = vm.isActive(data.status);
                vm.toggleScoringButtonText = (vm.status_toggle ? 'Deactivate Scoring' : 'Activate Scoring');
            });
        }
    }

    vm.init = function() {
        // console.log(vm.dashboard);
        // console.log(vm.ratingEngine);

        vm.initModalWindow();

        vm.relatedItems = vm.dashboard.plays;
        vm.isRulesBased = (vm.ratingEngine.type === 'RULE_BASED');
        vm.hasBuckets = vm.ratingEngine.counts != null;
        vm.statusIsActive = (vm.ratingEngine.status === 'ACTIVE');
        var model = vm.ratingEngine.activeModel;
        
        if (vm.isRulesBased) {

            console.log(vm.status_toggle);

            vm.toggleScoringButtonText = (vm.status_toggle ? 'Deactivate Scoring' : 'Activate Scoring');
            console.log(vm.toggleScoringButtonText);

        } else {

            var type = vm.ratingEngine.type.toLowerCase();

            if (type === 'cross_sell') {
                if ((Object.keys(model.AI.advancedModelingConfig[type].filters).length === 0 || (model.AI.advancedModelingConfig[type].filters['PURCHASED_BEFORE_PERIOD'] && Object.keys(model.AI.advancedModelingConfig[type].filters).length === 1)) && model.AI.trainingSegment === null && model.AI.advancedModelingConfig[type].filters.trainingProducts === null) {
                    vm.hasSettingsInfo = false;
                }

                console.log(model);

                vm.targetProducts = model.AI.advancedModelingConfig[type].targetProducts;
                vm.modelingStrategy = model.AI.advancedModelingConfig[type].modelingStrategy;
                vm.configFilters = model.AI.advancedModelingConfig[type].filters;
                vm.trainingProducts = model.AI.advancedModelingConfig[type].trainingProducts;

                if (vm.configFilters['SPEND_IN_PERIOD']) {
                    if (vm.configFilters['SPEND_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL') {
                        vm.spendCriteria = 'at least';
                    } else {
                        vm.spendCriteria = 'at most';
                    }
                }

                if (vm.configFilters['QUANTITY_IN_PERIOD']) {
                    if (vm.configFilters['QUANTITY_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL') {
                        vm.quantityCriteria = 'at least';
                    } else {
                        vm.quantityCriteria = 'at most';
                    }
                }

                if (vm.targetProducts !== null) {
                    vm.targetProductName = vm.returnProductNameFromId(vm.targetProducts[0]);
                }
                if (vm.trainingProducts !== null) {
                    vm.trainingProductName = vm.returnProductNameFromId(vm.trainingProducts[0]);
                }

                if (vm.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
                    vm.ratingEngineType = 'First Purchase Cross-Sell'
                } else if (vm.modelingStrategy === 'CROSS_SELL_REPEAT_PURCHASE') {
                    vm.ratingEngineType = 'Repeat Purchase Cross-Sell'
                }
            }

            vm.predictionType = model.AI.predictionType;
            vm.trainingSegment = model.AI.trainingSegment;

            if (vm.predictionType === 'PROPENSITY') {
                vm.prioritizeBy = 'Likely to buy';
            } else if (vm.predictionType === 'EXPECTED_VALUE') {
                vm.prioritizeBy = 'Likely to spend';
            }
        }

    }

    vm.returnProductNameFromId = function(productId) {
        var products = vm.products,
            product = products.find(function(obj) { return obj.ProductId === productId.toString() });

        return product.ProductName;
    };

    vm.init();
});
