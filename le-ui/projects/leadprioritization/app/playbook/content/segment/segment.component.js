angular.module('lp.playbook.wizard.segment', ['mainApp.appCommon.utilities.SegmentsUtility'])
.controller('PlaybookWizardSegment', function(
    $state, $stateParams, ResourceUtility, PlaybookWizardStore, SegmentsUtility, Segments
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.segment_form,
        SegmentsUtility: SegmentsUtility,
        segments: Segments
    });

    vm.init = function() {

    }

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        PlaybookWizardStore.setValidation('segment', form.$valid);
    }

    vm.saveSegment = function(segment) {
        PlaybookWizardStore.saveSegment(segment);
    }

    vm.init();
});