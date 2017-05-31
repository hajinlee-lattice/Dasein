angular.module('lp.playbook.wizard.segment', [])
.controller('PlaybookWizardSegment', function(
    $state, $stateParams, $scope, ResourceUtility, Segments
) {
    var vm = this;

    angular.extend(vm, {
        segments: Segments
    });

    vm.init = function() {
    }

    vm.init();
});