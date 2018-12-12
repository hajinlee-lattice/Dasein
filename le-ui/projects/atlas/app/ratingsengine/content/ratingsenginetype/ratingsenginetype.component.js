angular.module('lp.ratingsengine.ratingsenginetype', [])
.controller('RatingsEngineType', function ($state, $stateParams, 
    RatingsEngineStore, FeatureFlagService, ConfigureAttributesStore, DataCollectionStatus) {
    var vm = this;
    angular.extend(vm, {
        datacollectionPrecheck: null,
        datacollectionPrechecking: false,
        periodType: DataCollectionStatus.ApsRollingPeriod
    });

    function getDatacollectionPrecheck() {
        vm.datacollectionPrechecking = true; // spinner
        ConfigureAttributesStore.getPrecheck().then(function(result) {
            vm.datacollectionPrecheck = result;
            vm.datacollectionPrechecking = false;
        });
    }

    vm.setType = function(wizardSteps, engineType) {
        // RatingsEngineStore.setType(type, engineType);
        $state.go('home.ratingsengine.' + wizardSteps, {engineType: engineType});
    }

    var flags = FeatureFlagService.Flags();
    vm.showCrossSellModeling = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CROSS_SELL_MODELING);

    vm.init = function() {
        getDatacollectionPrecheck();
    };
    vm.init();
});