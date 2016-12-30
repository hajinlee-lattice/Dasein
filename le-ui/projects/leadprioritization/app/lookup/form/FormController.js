angular
.module('lp.lookup.form', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('LookupFormController', function(
    $state, LookupStore, ResourceUtility, FeatureFlagService
) {

    var validFormStates = [
        ['Website'],
        ['CompanyName', 'City', 'State', 'Country']
    ];

    var vm = this;

    angular.extend(vm, {
        request: LookupStore.get('request'),
        params: LookupStore.get('params'),
        ResourceUtility: ResourceUtility,
        requiredMissingField: {
            Website: true,
            CompanyName: true,
            City: true,
            State: true,
            Country: true
        },
        formIsValid: false
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

        vm.requiredMissingField = {};

        for (var i = 0; i < validFormStates.length; i++) {
            var state = validFormStates[i];
            var valid = true;
            var dirty = false;
            var stateMissingField = {};

            for (var j = 0; j < state.length; j++) {
                var field = state[j];

                if (!vm.request.record[field]) {
                    vm.requiredMissingField[field] = true;
                    stateMissingField[field] = true;
                    valid = false;
                } else {
                    dirty = true;
                }
            }

            if (valid) {
                return true;
            }

            if (dirty) {
                vm.requiredMissingField = stateMissingField;
                return false;
            }
        }

        return false;
    }
});