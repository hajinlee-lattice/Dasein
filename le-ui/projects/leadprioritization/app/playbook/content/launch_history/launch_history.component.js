angular.module('lp.playbook.dashboard.launch_history', [])
.controller('PlaybookDashboardLaunchHistory', function(
    $state, $stateParams, $timeout, ResourceUtility, PlaybookWizardStore, LaunchHistoryData
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.settings_form,
        launches: LaunchHistoryData,
        cumulativeStats: LaunchHistoryData.cumulativeStats,
        summaryData: {},
        launching: false,
        current: 1,
        pagesize: 10,
        showPagination: false
    });

    vm.init = function() {

        // console.log(vm.launches);

        if(vm.launches.launchSummaries.length > 10){
            vm.showPagination = true;
        }

        vm.defaultPlayLaunchList = angular.copy(vm.launches.uniquePlaysWithLaunches);
        vm.defaultPlayLaunchList.unshift({playName: null, displayName: 'All Launched Plays'});

        if($state.current.name === 'home.playbook.plays.launch_history'){
            vm.allPlaysHistory = true;
        } else {
            vm.allPlaysHistory = false;
        }

        vm.summaryData = {
            selectedTargets: vm.cumulativeStats.selectedTargets,
            suppressed: vm.cumulativeStats.suppressed,
            errors: vm.cumulativeStats.errors,
            recommendationsLaunched: vm.cumulativeStats.recommendationsLaunched,
            contactsWithinRecommendations: vm.cumulativeStats.contactsWithinRecommendations
        }

    };

    vm.playSelectChange = function(play){

        console.log(play);

        var playName = null;

        if(play === {}){
            playName = null;
        } else {
            playName = play.undefined.name;
        }

        console.log(play);

        var params = {
            playName: playName
        }

        PlaybookWizardStore.getPlayLaunches(params).then(function(result){
            vm.launches = result;
        });
    };

    vm.play_name_required = function(){
        return !vm.stored.play_display_name;
    };

    vm.relaunchPlay = function() {

        vm.launching = true;

        PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
            PlaybookWizardStore.launchPlay(play).then(function(result){
                $state.reload();
            });
        });
    };

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