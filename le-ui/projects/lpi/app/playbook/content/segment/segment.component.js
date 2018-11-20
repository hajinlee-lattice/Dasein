angular.module('lp.playbook.wizard.segment', ['mainApp.appCommon.utilities.SegmentsUtility'])
.controller('PlaybookWizardSegment', function(
    $scope, $state, $stateParams, ResourceUtility, PlaybookWizardStore, SegmentsUtility, Segments
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.segment_form,
        SegmentsUtility: SegmentsUtility,
        segments: Segments,
        stateParams: $stateParams,
        currentPage: 1,
        pageSize: 20
    });

    $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
        }
    });

    vm.init = function() {
        PlaybookWizardStore.setValidation('segment', false);
        if($stateParams.play_name) {
            PlaybookWizardStore.setValidation('settings', true);
            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
                vm.savedSegment = play.segment;
                vm.stored.segment_selection = play.segment;
                if(play.segment) {
                    PlaybookWizardStore.setValidation('segment', true);
                }
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
        if(vm.stored.segment_selection) {
            PlaybookWizardStore.setSettings({
                segment: vm.stored.segment_selection
            });
        }
    }

    vm.saveSegment = function(segment) {
        PlaybookWizardStore.setSegment(segment);
    }

    vm.savePlay = function() {
        var segment = PlaybookWizardStore.getSavedSegment().name,
            play_name = $stateParams.play_name,
            play = PlaybookWizardStore.getCurrentPlay();

        play.segment = segment;
        PlaybookWizardStore.savePlay(play).then(function(result) {
             $state.go('home.playbook.dashboard', {play_name: play.name});
        });
    }

    vm.init();
});