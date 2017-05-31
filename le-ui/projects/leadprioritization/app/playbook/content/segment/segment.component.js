angular.module('lp.playbook.wizard.segment', ['mainApp.appCommon.utilities.SegmentsUtility'])
.controller('PlaybookWizardSegment', function(
    $state, $stateParams, $scope, ResourceUtility, SegmentsUtility, Segments
) {
    var vm = this;

    angular.extend(vm, {
        SegmentsUtility: SegmentsUtility,
        segments: Segments
    });

    vm.init = function() {
    }

    vm.init();
});