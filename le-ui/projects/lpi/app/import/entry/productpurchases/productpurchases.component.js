angular.module('lp.import.entry.productpurchases', [])
.component('productPurchasesContent', {
    templateUrl: 'app/import/entry/productpurchases/productpurchases.component.html',
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