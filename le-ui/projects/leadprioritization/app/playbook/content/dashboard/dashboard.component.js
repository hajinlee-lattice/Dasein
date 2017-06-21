angular.module('lp.playbook.dashboard', [])
.controller('PlaybookDashboard', function(
    $stateParams, PlaybookWizardStore
) {
    var vm = this;

    angular.extend(vm, {
        play: null
    });
        PlaybookWizardStore.clear();
        if($stateParams.play_name) {
            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
                vm.play = play;
           });
        }

});
