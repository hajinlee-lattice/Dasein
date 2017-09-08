angular.module('common.datacloud.analysistabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('AnalysisTabsController', function (
    $state, $stateParams, $timeout, $scope, FeatureFlagService, BrowserStorageUtility,
    ResourceUtility, DataCloudStore, QueryService, QueryStore
) {
    var vm = this,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        DataCloudStore: DataCloudStore,
        QueryStore: QueryStore,
        stateParams: $stateParams,
        segment: $stateParams.segment,
        section: $stateParams.section,
        show_lattice_insights: FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS),
        accountRestriction: QueryStore.getAccountRestriction() || null,
        contactRestriction: QueryStore.getContactRestriction() || null,
        counts: QueryStore.getCounts(),
        accountsCount: 0,
        contactsCount: 0
    });

    vm.init = function() {

        $timeout(function(){
            QueryStore.setResourceTypeCount('accounts', false, vm.accountsCount);
            QueryStore.setResourceTypeCount('contacts', false, vm.contactsCount);
        }, 2000);

        QueryStore.history = [];

        QueryStore.GetCountByQuery('accounts').then(function(data){ 
            vm.accountsCount = data;
        });
        QueryStore.GetCountByQuery('contacts').then(function(data){ 
            vm.contactsCount = data;
        });

        if(vm.segment === 'Create'){
            var attributesUrl = "home.segment.explorer.attributes({segment:'Create'})";
        } else {
            var attributesUrl = "home.segment.explorer.attributes({segment:'" + vm.segment + "'})";
        }
        

        vm.attributes = vm.inModel()
            ? 'home.model.analysis.explorer'
            : attributesUrl;

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

        var segment = {
            segment: vm.segment
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