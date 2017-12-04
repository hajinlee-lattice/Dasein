angular
.module('pd.navigation.sidebar.root', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.services.FeatureFlagService',
    'common.datacloud'
])
.controller('SidebarRootController', function(
    $state, $stateParams, StateHistory, FeatureFlagService, ResourceUtility, DataCloudStore
) {
    var vm = this;

    angular.extend(vm, {
        state: $state,
        ResourceUtility: ResourceUtility,
        stateParams: $stateParams,
        StateHistory: StateHistory,
        MyDataStates: [
            'home.nodata',
            'home.segment.explorer.attributes',
            'home.segment.explorer.builder',
            'home.segment.accounts',
            'home.segment.contacts'
        ]
    });

    vm.init = function() {
        vm.isDataAvailable = DataCloudStore.metadata.enrichmentsTotal > 0;

        FeatureFlagService.GetAllFlags().then(function(result) {
            var flags = FeatureFlagService.Flags();
            
            vm.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            vm.showModelCreationHistory = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
            vm.showApiConsole = FeatureFlagService.FlagIsEnabled(flags.API_CONSOLE_PAGE);
            vm.showMarketoSettings = FeatureFlagService.FlagIsEnabled(flags.USE_MARKETO_SETTINGS);
            vm.showEloquaSettings = FeatureFlagService.FlagIsEnabled(flags.USE_ELOQUA_SETTINGS);
            vm.showSalesforceSettings = FeatureFlagService.FlagIsEnabled(flags.USE_SALESFORCE_SETTINGS);
            vm.showCampaignsPage = FeatureFlagService.FlagIsEnabled(flags.CAMPAIGNS_PAGE);
            vm.showAnalysisPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            vm.showPlayBook = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            vm.showRatingsEngine = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            vm.showSegmentationPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            vm.showCdlEnabledPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            vm.showLatticeInsightsPage = FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS);
            vm.showContactUs = false;
        });
    }

    vm.getMyDataState = function() {
        return vm.isDataAvailable ? "home.segment.explorer.attributes" : "home.nodata";
    }

    vm.checkMyDataActiveState = function() {
        return vm.isStateName(vm.MyDataStates) && (!vm.stateParams.segment || vm.stateParams.segment == 'Create')
    }

    vm.checkSegmentationActiveState = function() {
        return vm.isStateName(vm.MyDataStates) && (vm.stateParams.segment && vm.stateParams.segment != 'Create') || vm.state.current.name == 'home.segments';
    }

    vm.checkToState = function(toState) {
        return StateHistory.lastTo().name == toState;
    }

    vm.isStateName = function(state_names) {
        return (state_names || []).indexOf($state.current.name) !== -1; 
    }

    vm.isTransitingFrom = function(state_names) {
        return false;
        return (state_names || []).indexOf(StateHistory.lastFrom().name) !== -1 && (state_names || []).indexOf(StateHistory.lastTo().name) !== -1; 
    }

    vm.isTransitingTo = function(state_names) {
        return false;
        return (state_names || []).indexOf(StateHistory.lastTo().name) !== -1 && (state_names || []).indexOf($state.current.name) === -1; 
    }

    vm.isTransitingToMyData = function(state_names) {
        return false;
        return (state_names || []).indexOf($state.current.name) === -1 && vm.isTransitingTo(state_names) && (!StateHistory.lastToParams().segment || StateHistory.lastToParams().segment == 'Create');
    }

    vm.isTransitingToSegmentation = function(state_names) {
        return false;
        return vm.isTransitingTo(state_names) && (StateHistory.lastToParams().segment && StateHistory.lastToParams().segment != 'Create') || ('home.segments' !== $state.current.name && StateHistory.lastTo().name === 'home.segments');
    }

    vm.init();
});