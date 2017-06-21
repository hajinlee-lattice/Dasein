angular.module('lp.playbook.dashboard', [])
.controller('PlaybookDashboard', function(
    $stateParams, PlaybookWizardStore, CgTalkingPointStore, TalkingPointAttributes, TalkingPoints
) {
    var vm = this;

    angular.extend(vm, {
        play: null,
        talkingPointsAttributes: TalkingPointAttributes,
        talkingPoints: TalkingPoints
    });

    PlaybookWizardStore.clear();
    if($stateParams.play_name) {
        PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
            vm.play = play;
        });
    }

});
