angular.module('lp.playbook.dashboard', [
    'mainApp.appCommon.utilities.TimestampIntervalUtility'
])
.controller('PlaybookDashboard', function(
    $stateParams, PlaybookWizardStore, TimestampIntervalUtility, CgTalkingPointStore, TalkingPointAttributes, TalkingPoints
) {
    var vm = this;

    angular.extend(vm, {
        TimestampIntervalUtility: TimestampIntervalUtility,
        play: null,
        talkingPointsAttributes: TalkingPointAttributes,
        talkingPoints: TalkingPoints
    });

    PlaybookWizardStore.clear();
    if($stateParams.play_name) {
        PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
            vm.play = play;
            console.log({
                timestamp: play.timestamp, 
                ago: TimestampIntervalUtility.timeAgo(play.timestamp), 
                date: new Date(play.timestamp),
                gtm: new Date(play.timestamp).toGMTString()
            });
        });
    }

});
