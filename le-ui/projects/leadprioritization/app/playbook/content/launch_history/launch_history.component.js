angular.module('lp.playbook.dashboard.launch_history', [])
.controller('PlaybookDashboardLaunchHistory', function(
    $state, $stateParams, $timeout, ResourceUtility, PlaybookWizardStore, LaunchHistoryData
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.settings_form,
        launches: LaunchHistoryData,
        suppressed: 0,
        totalRecoGen: 0,
        errors: 0,
        expired: 0,
        disqualified: 0,
        totalSuppressed: 0
    });

    vm.init = function() {
        if($stateParams.play_name) {
            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){

                console.log(play);

                vm.stored.play_name = play.name;
                vm.stored.play_display_name = play.displayName;
                vm.stored.play_description = play.description;
                if(vm.stored.play_name) {
                    PlaybookWizardStore.setValidation('settings', true);
                }


                for (var i = 0; i < vm.launches.length; i++) {
                    vm.totalRecoGen = vm.totalRecoGen + vm.launches[i].accountsNum;
                    vm.totalSuppressed = 0;
                }


           });
        }

        vm.totalSuppressed = function(){
            
        };

        console.log(vm.launches);

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