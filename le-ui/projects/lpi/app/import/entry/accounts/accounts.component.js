angular.module('lp.import.entry.accounts', [])
.component('accountsContent', {
    templateUrl: 'app/import/entry/accounts/accounts.component.html',
    controller: function (
        $q, $scope, $stateParams, ResourceUtility
    ) {

        var vm = this;

        angular.extend(vm, {
            ResourceUtility: ResourceUtility,
            action: $stateParams.action
        });

        vm.$onInit = function() {
            
        }

    }
});