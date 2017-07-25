angular.module('common.datacloud.analysistabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('AnalysisTabsController', function (
    $state, $stateParams, $scope, FeatureFlagService, BrowserStorageUtility,
    ResourceUtility, DataCloudStore, QueryService, QueryStore
) {
    var vm = this,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        DataCloudStore: DataCloudStore,
        stateParams: $stateParams,
        section: $stateParams.section,
        show_lattice_insights: FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS),
        restriction: QueryStore.getRestriction(),
        counts: QueryService.GetCountByRestriction('accounts', {'restriction': vm.restriction})
    });

    vm.init = function() {

        console.log(vm.restriction);

        vm.attributes = vm.inModel()
            ? 'home.model.analysis.explorer'
            : 'home.segment.explorer';

        vm.accounts = vm.inModel()
            ? 'home.model.analysis.accounts'
            : 'home.segment.accounts';

        vm.contacts = vm.inModel()
            ? 'home.model.analysis.contacts'
            : 'home.segment.contacts';

    }

    vm.inModel = function() {
        var name = $state.current.name.split('.');
        return name[1] == 'model';
    }

    vm.setStateParams = function(section) {
        var goHome = false;

        if (section && section == vm.section && section) {
            goHome = true;
        }

        vm.section = section;

        var params = {
            section: vm.section
        };

        if (goHome) {
            params.category = '';
            params.subcategory = '';
        }

        if (vm.inModel()) {
            $state.go('home.model.analysis', params, { notify: true });
        } else {
            $state.go('home.segment', params, { notify: true });
        }
    }

    vm.init();
});