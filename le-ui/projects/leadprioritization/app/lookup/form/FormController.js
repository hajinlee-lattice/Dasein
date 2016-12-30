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
        ResourceUtility: ResourceUtility
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

        if (vm.request.record.Website) {
            vm.requiredMissingField = {};
            return true;
        }

        if (vm.request.record.CompanyName ||
            vm.request.record.City ||
            vm.request.record.State ||
            vm.request.record.Country) {
            vm.requiredMissingField = {
                CompanyName: !vm.request.record.CompanyName,
                City: !vm.request.record.City,
                State: !vm.request.record.State,
                Country: !vm.request.record.Country
            };

            return _.reduce(vm.requiredMissingField, function (valid, value, field) {
                return valid && !value;
            }, true);
        }

        vm.requiredMissingField = {
            Website: true,
            CompanyName: true,
            City: true,
            State: true,
            Country: true
        };
        return false;
    }
});