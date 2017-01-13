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

        if (vm.request.record.Website || vm.request.record.CompanyName) {
            vm.requiredMissingField = {};
            return true;
        }

        vm.requiredMissingField = {
            Website: true,
            CompanyName: true
        };

        return false;
    }
});