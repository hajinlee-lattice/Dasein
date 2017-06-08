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
        if($stateParams.play_name) {
            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
                console.log(play);
                vm.savedSegment = play.segment;
                vm.stored.segment_selection = play.segment;
            });
        }
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
        PlaybookWizardStore.saveSegment(segment, $stateParams.play_name);
    }

    vm.init();
});