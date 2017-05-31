angular.module('lp.playbook.wizard.segment', ['mainApp.appCommon.utilities.SegmentUtility'])
.controller('PlaybookWizardSegment', function(
    $state, $stateParams, $scope, ResourceUtility, SegmentUtility, Segments
) {
    var vm = this;

    angular.extend(vm, {
        SegmentUtility: SegmentUtility,
        segments: Segments
    });

    vm.init = function() {
    }

    vm.init();
});