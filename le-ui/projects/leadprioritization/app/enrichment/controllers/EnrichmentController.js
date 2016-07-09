angular.module('lp.enrichment.leadenrichment', [])
.controller('EnrichmentController', function($scope, EnrichmentStore){
    var vm = $scope;
    angular.extend(vm, {
        button_save: 'save'
    });

    vm.init = function() {
        EnrichmentStore.get().then(function(result){
            angular.extend(vm, result);
        });
    }

    vm.init();

});
