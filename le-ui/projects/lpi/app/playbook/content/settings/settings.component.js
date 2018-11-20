angular.module('lp.playbook.wizard.settings', [])
.controller('PlaybookWizardSettings', function(
    $state, $stateParams, $timeout, ResourceUtility, PlaybookWizardStore
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.settings_form,
    });

    vm.init = function() {
        if($stateParams.play_name) {
            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
                vm.stored.play_name = play.name;
                vm.stored.play_display_name = play.displayName;
                vm.stored.play_description = play.description;
                if(vm.stored.play_name) {
                    PlaybookWizardStore.setValidation('settings', true);
                }
           });
        }
    };

    vm.play_name_required = function(){
        return !vm.stored.play_display_name;
    }

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        PlaybookWizardStore.setValidation('settings', form.$valid);
        if(vm.stored.play_display_name) {
            PlaybookWizardStore.setSettings({
                displayName: vm.stored.play_display_name,
                description: vm.stored.play_description
            });
        }
    }

    vm.init();
});