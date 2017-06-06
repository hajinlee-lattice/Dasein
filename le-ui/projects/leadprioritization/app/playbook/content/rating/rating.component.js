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
        
    }

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        PlaybookWizardStore.setValidation('rating', form.$valid);
    }

    vm.init();
});