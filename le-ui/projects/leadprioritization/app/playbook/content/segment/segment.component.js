angular.module('lp.playbook.wizard.segment', ['mainApp.appCommon.utilities.SegmentsUtility'])
.controller('PlaybookWizardSegment', function(
    $state, $stateParams, $scope, ResourceUtility, PlaybookWizardStore, SegmentsUtility, Segments
) {
    var vm = this;

    angular.extend(vm, {
        SegmentsUtility: SegmentsUtility,
        segments: Segments
    });

    vm.init = function() {
    }

    vm.saveSegment = function(segment) {
        PlaybookWizardStore.saveSegment(segment);
    }

    vm.init();
});