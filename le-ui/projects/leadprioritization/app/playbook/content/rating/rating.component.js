angular.module('lp.playbook.wizard.rating', [])
.controller('PlaybookWizardRating', function(
    $state, $stateParams, ResourceUtility, Ratings, PlaybookWizardStore
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.rating_form,
        ratings: Ratings
    });

    vm.init = function() {
        PlaybookWizardStore.setValidation('rating', false);
        if(vm.stored.rating_selection) {
            PlaybookWizardStore.setValidation('rating', false);
        }
        if($stateParams.play_name) {
            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
            });
        }
    }

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        PlaybookWizardStore.setValidation('rating', form.$valid);
        if(vm.stored.rating_selection) {
            PlaybookWizardStore.setRating(vm.stored.rating_selection);
        }
    }

    vm.init();
});