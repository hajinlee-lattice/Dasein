angular.module('lp.import.entry.productbundles', [])
.component('productBundlesContent', {
    templateUrl: 'app/import/entry/productbundles/productbundles.component.html',
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