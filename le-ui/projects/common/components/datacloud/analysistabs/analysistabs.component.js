angular.module('common.datacloud.analysistabs', [])
.controller('AnalysisTabsController', function (
    $state, $stateParams, FeatureFlagService, DataCloudStore, QueryStore, StateHistory
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
        counts: QueryStore.getCounts()
    });

    vm.init = function() {
        QueryStore.history = [];

        QueryStore.getEntitiesCounts();

        var attributesUrl = "home.segment.explorer.attributes({segment:'" + vm.segment + "'})";

        vm.attributes = vm.ifInModel('home.model.analysis.explorer', attributesUrl);
        vm.accounts = vm.ifInModel('home.model.analysis.accounts', 'home.segment.accounts');
        vm.contacts = vm.ifInModel('home.model.analysis.contacts', 'home.segment.contacts');
    }

    vm.getRuleCount = function() {
        var all = [];

        ['accountRestriction','contactRestriction'].forEach(function(source) {
            if (QueryStore[source].restriction) {
                buckets = QueryStore.getAllBuckets(QueryStore[source].restriction.logicalRestriction.restrictions)
                all = [].concat(all, buckets);
            }
        });

        return all.length || 0;
    }

    vm.checkState = function(type) {
        var map = {
            'home.model.analysis.explorer.builder':'builder',
            'home.segment.explorer.builder':'builder',
            'home.model.analysis.explorer.attributes':'attributes',
            'home.segment.explorer.attributes':'attributes',
            'home.segment.accounts':'accounts',
            'home.segment.contacts':'contacts'
        };

        return map[StateHistory.lastTo().name] == type;
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

        var state = vm.ifInModel('home.model.analysis', 'home.segment');

        $state.go(state, params, { notify: true });
    }

    vm.inModel = function() {
        var name = $state.current.name.split('.');
        return name[1] == 'model';
    }

    vm.ifInModel = function(model, not) {
        return vm.inModel() ? model : not;
    }

    vm.go = function(state) {
        $state.go(state);
    }

    vm.init();
});