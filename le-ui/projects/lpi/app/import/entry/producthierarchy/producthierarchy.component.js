angular.module('lp.import.entry.producthierarchy', [])
.component('productHierarchyContent', {
    templateUrl: 'app/import/entry/producthierarchy/producthierarchy.component.html',
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