angular
.module('common.datacloud.lookup', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.directives.ngEnterDirective'
])
.controller('LookupController', function(
    $state, $stateParams, LookupStore, ResourceUtility, FeatureFlagService
) {
    var vm = this;

    angular.extend(vm, {
        request: LookupStore.get('request'),
        params: LookupStore.get('params'),
        ResourceUtility: ResourceUtility,
        iframeMode: $stateParams.iframe
    });

    vm.cancel = function() {
        $state.go('home.datacloud.datacloud');
    }

    vm.next = function() {
        var timestamp = new Date().getTime();

        if (vm.iframeMode) {
            LookupStore.add('Authentication', vm.Authentication);
        }

        LookupStore.add('timestamp', timestamp);
        LookupStore.add('request', vm.request);
        LookupStore.add('params', vm.params);

        if (vm.iframeMode) {
            $state.go('home.insights.iframe');
        } else {
            $state.go('home.datacloud.lookup.tabs');
        }
    }

    vm.parse = function() {
        try { 
            var json = JSON.parse(vm.json);
            vm.request.record = json.request.record;
            vm.Authentication = json.Authentication; 
        } catch(e) {
            console.log('JSON Parse Error:',e);
            vm.request.record = {};
        }
    }

    vm.validate = function() {
        if (vm.request.record.Website || vm.request.record.CompanyName) {
            vm.requiredMissingField = {};

            if (vm.iframeMode && !vm.Authentication) {
                vm.requiredMissingField.Authentication = true;
            }

            return true;
        }

        vm.requiredMissingField = {
            Website: true,
            CompanyName: true
        };

        if (vm.iframeMode && !vm.Authentication) {
            vm.requiredMissingField.Authentication = true;
        }

        return false;
    }
});