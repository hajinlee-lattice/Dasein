angular
.module('lp.lookup.form', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('LookupFormController', function(
    $state, LookupStore, ResourceUtility, FeatureFlagService
) {
    var vm = this;

    angular.extend(vm, {
        request: LookupStore.get('request'),
        params: LookupStore.get('params'),
        ResourceUtility: ResourceUtility,
        requiredMissingField: {},
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
        if (!vm.validate()) {
            return;
        }

        var timestamp = new Date().getTime();

        LookupStore.add('timestamp', timestamp);
        LookupStore.add('request', vm.request);
        LookupStore.add('params', vm.params);
        
        $state.go('home.lookup.tabs');
    }

    vm.validate = function() {
        var validFormStates = [
            ['Website'],
            ['CompanyName', 'City', 'State', 'Country']
        ];

        vm.formIsValid = false;

        if (vm.request.record.Website) {
            vm.formIsValid = true;
        } else if (vm.request.record.CompanyName ||
            vm.request.record.City ||
            vm.request.record.State || 
            vm.request.record.Zip) {
            vm.requiredMissingField = {};

            validFormStates[1].forEach(function(field)  {
                if (!vm.request.record[field]) {
                    vm.requiredMissingField[field] = true;
                    vm.formIsValid = false;
                }
            });
        }

        return vm.formIsValid || Object.keys(vm.requiredMissingField) === 0;
    }
});