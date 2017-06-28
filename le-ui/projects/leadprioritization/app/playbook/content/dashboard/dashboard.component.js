angular.module('lp.playbook.dashboard', [
    'mainApp.appCommon.utilities.TimestampIntervalUtility'
])
.controller('PlaybookDashboard', function(
    $q, $stateParams, PlaybookWizardStore, TimestampIntervalUtility, CgTalkingPointStore, TalkingPointAttributes, TalkingPoints
) {
    var vm = this,
        play_name = $stateParams.play_name;

    angular.extend(vm, {
        TimestampIntervalUtility: TimestampIntervalUtility,
        play: null,
        talkingPointsAttributes: TalkingPointAttributes,
        talkingPoints: TalkingPoints
    });

    $q.when($stateParams.play_name, function() {
        if(play_name) {
            CgTalkingPointStore.getTalkingPoints(play_name).then(function(results){
                console.log('got talking points');
            });
        }

    });

    PlaybookWizardStore.clear();
    if(play_name) {
        PlaybookWizardStore.getPlay(play_name).then(function(play){
            vm.play = play;
        });
    }

});
