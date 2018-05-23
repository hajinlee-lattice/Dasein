angular
.module('pd.navigation.sidebar.model', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.services.FeatureFlagService',
    'lp.ratingsengine'
])
.controller('SidebarModelController', function(
    $rootScope, $state, $stateParams, FeatureFlagService, ResourceUtility, 
    StateHistory, Model, IsPmml, IsRatingEngine, RatingEngine, HasRatingsAvailable
) {
    var vm = this;

    angular.extend(vm, {
        state: $state,
        ResourceUtility: ResourceUtility,
        stateParams: $stateParams,
        StateHistory: StateHistory,
        model: Model,
        ratingEngine: RatingEngine
    });

    vm.init = function() {

        vm.IsPmml = IsPmml,
        vm.IsRatingEngine = IsRatingEngine,
        vm.sourceType = Model.ModelDetails.SourceSchemaInterpretation;
        vm.Uploaded = Model.ModelDetails.Uploaded;
        vm.HasRatingsAvailable = HasRatingsAvailable;

        // console.log(vm.ratingEngine);
        if (!vm.IsRatingEngine) {
            $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });
        }

        if (JSON.stringify(vm.HasRatingsAvailable) != "{}") {
            vm.HasRatingsAvailable = true;
        } else {
            vm.HasRatingsAvailable = false;
        }

        FeatureFlagService.GetAllFlags().then(function(result) {
            var flags = FeatureFlagService.Flags();
            
            vm.showAnalysisPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            vm.showSegmentationPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            vm.showCdlEnabledPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            vm.canRemodel = FeatureFlagService.FlagIsEnabled(flags.VIEW_REMODEL) && !vm.IsPmml && !vm.Uploaded;
            vm.showModelSummary = FeatureFlagService.FlagIsEnabled(flags.ADMIN_PAGE) || FeatureFlagService.UserIs('EXTERNAL_ADMIN');
            vm.showAlerts = 0; // disable for all (PLS-1670) FeatureFlagService.FlagIsEnabled(flags.ADMIN_ALERTS_TAB);
            vm.showRefineAndClone = FeatureFlagService.FlagIsEnabled(flags.VIEW_REFINE_CLONE);
            vm.showReviewModel = FeatureFlagService.FlagIsEnabled(flags.REVIEW_MODEL);
            vm.showSampleLeads = FeatureFlagService.FlagIsEnabled(flags.VIEW_SAMPLE_LEADS);
        });
    }

    vm.goToRatingEngineRoute = function() {
        $state.go('home.ratingsengine.list');
    }

    vm.checkToState = function(toState) {
        return StateHistory.lastTo().name == toState;
    }

    vm.isStateName = function(state_names) {
        return (state_names || []).indexOf($state.current.name) !== -1; 
    }

    vm.isTransitingFrom = function(state_names) {
        return (state_names || []).indexOf(StateHistory.lastFrom().name) !== -1 && (state_names || []).indexOf(StateHistory.lastTo().name) !== -1; 
    }

    vm.isTransitingTo = function(state_names) {
        return (state_names || []).indexOf(StateHistory.lastTo().name) !== -1 && (state_names || []).indexOf($state.current.name) === -1; 
    }

    vm.init();
});