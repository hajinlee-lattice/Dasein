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
            PlaybookWizardStore.getPlayLaunches(play_name).then(function(results){
                vm.launchHistory = results;
            });

            CgTalkingPointStore.getTalkingPoints(play_name).then(function(results){
                vm.talkingPoints = results || [];
            });
        }

    });

    vm.makeRatingsGraph = function(ratings) {
        ratings = [{
            bucket: "A",
            count: 1
        },{
            bucket: "B",
            count: 2
        },{
            bucket: "C",
            count: 5
        },{
            bucket: "D",
            count: 4
        }];

        var total =  0;
        for (var i in ratings) {
            total += ratings[i].count;
        }

        return {
            total: total,
            ratings: ratings
        }
    };

    PlaybookWizardStore.clear();
    if(play_name) {
        PlaybookWizardStore.getPlay(play_name).then(function(play){
            vm.play = play;
            vm.ratingsGraph = vm.makeRatingsGraph(vm.play.rating);
        });
    }

});
