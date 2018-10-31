angular.module('lp.import.entry.contacts', [])
.component('contactsContent', {
    templateUrl: 'app/import/entry/contacts/contacts.component.html',
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