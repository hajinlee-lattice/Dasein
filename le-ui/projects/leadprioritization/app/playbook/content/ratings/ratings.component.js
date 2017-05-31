angular.module('lp.playbook.wizard.ratings', [])
.controller('PlaybookWizardRatings', function(
    $state, $stateParams, $scope, ResourceUtility, Ratings
) {
    var vm = this;

    angular.extend(vm, {
        ratings: Ratings
    });

    vm.init = function() {
        
    }

    vm.init();
});