angular
.module('lp.lookup.form', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('LookupFormController', function(
    $state, LookupStore, Models, ResourceUtility, FeatureFlagService
) {
    var vm = this;

    angular.extend(vm, {
        request: LookupStore.get('request'),
        params: LookupStore.get('params'),
        ResourceUtility: ResourceUtility,
        models: Models
    });

    FeatureFlagService.GetAllFlags().then(function(result) {
        var flags = FeatureFlagService.Flags();
        LookupStore.setParam('enforceFuzzyMatch', FeatureFlagService.FlagIsEnabled(flags.ENABLE_FUZZY_MATCH));
        vm.params = LookupStore.get('params');
        vm.EnableFuzzyMatch = vm.params.enforceFuzzyMatch;
    });

    vm.cancel = function() {
        $state.go('home.enrichments');
    }

    vm.next = function() {
        var timestamp = new Date().getTime();

        LookupStore.add('timestamp', timestamp);
        LookupStore.add('request', vm.request);
        LookupStore.add('params', vm.params);
        
        $state.go('home.lookup.tabs');
    }

    vm.validate = function() {
        var validFormStates = [
            ['CompanyName', 'City', 'State', 'Country'],
            ['Website']
        ];

        for (var i = 0; i < validFormStates.length; i++) {
            var state = validFormStates[i];
            var valid = true;

            for (var j = 0; j < state.length; j++) {
                var key = state[j];
                if (!vm.request.record[key]) {
                    valid = false;
                    dirty = true;
                    break;
                }
            }

            if (valid) {
                return true;
            } else if (dirty) {
                // hightlight required fields
            }
        }

        return false;
    }
});