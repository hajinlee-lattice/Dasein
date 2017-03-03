angular
.module('common.datacloud.lookup', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.directives.ngEnterDirective'
])
.controller('LookupController', function(
    $state, LookupStore, ResourceUtility, FeatureFlagService
) {
    var vm = this;

    angular.extend(vm, {
        request: LookupStore.get('request'),
        params: LookupStore.get('params'),
        ResourceUtility: ResourceUtility
    });

    vm.cancel = function() {
        $state.go('home.datacloud.datacloud');
    }

    vm.next = function() {
        var timestamp = new Date().getTime();

        LookupStore.add('timestamp', timestamp);
        LookupStore.add('request', vm.request);
        LookupStore.add('params', vm.params);

        $state.go('home.datacloud.lookup.tabs');
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