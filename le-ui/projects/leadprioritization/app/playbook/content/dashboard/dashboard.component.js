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

    vm.removeSegment = function(play) {
        PlaybookWizardStore.removeSegment(play);
    }

    vm.makeRatingsGraph = function(ratings) {
        ratings = [{
            bucket: "A",
            count: 105
        },{
            bucket: "B",
            count: 132
        },{
            bucket: "C",
            count: 244
        },{
            bucket: "D",
            count: 512
        },{
            bucket: "F",
            count: 680
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
