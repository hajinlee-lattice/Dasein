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
        showPagination: false,
        sortBy: 'created',
        sortDesc: true
    });

    vm.init = function() {

        if(vm.launches.launchSummaries.length > 10){
            vm.showPagination = true;
        } else {
            vm.showPagination = false;
        }

        vm.defaultPlayLaunchList = angular.copy(vm.launches.uniquePlaysWithLaunches);
        vm.defaultPlayLaunchList.unshift({playName: null, displayName: 'All Launched Plays'});

        if($state.current.name === 'home.playbook.plays.launch_history'){
            vm.allPlaysHistory = true;
        } else {
            vm.allPlaysHistory = false;
        }


        for(var i = 0; i < vm.launches.launchSummaries.length; i++) {
            if (vm.launches.launchSummaries[i].launchState == 'Launching') {
                vm.launching = true;
                break;
            }
        }


        vm.summaryData = {
            selectedTargets: vm.cumulativeStats.selectedTargets,
            suppressed: vm.cumulativeStats.suppressed,
            errors: vm.cumulativeStats.errors,
            recommendationsLaunched: vm.cumulativeStats.recommendationsLaunched,
            contactsWithinRecommendations: vm.cumulativeStats.contactsWithinRecommendations
        }

        console.log(vm.launches.launchSummaries.length, vm.showPagination);

    };

    vm.sort = function(header) {

        vm.sortBy = header;

        var params = {
            playName: $stateParams.play_name,
            sortby: header,
            descending: vm.sortDesc
        };

        console.log(vm.sortBy, vm.sortDesc);

        PlaybookWizardStore.getPlayLaunches(params).then(function(result){
            console.log(result);
            vm.launches = result;
        });

    }

    vm.playSelectChange = function(play){

        console.log(play);

        var playName = null;

        if(play === undefined){
            playName = null;
        } else {
            playName = play.undefined.name;
        }

        var params = {
            playName: playName
        }

        PlaybookWizardStore.getPlayLaunches(params).then(function(result){
            vm.launches = result;

            if(vm.launches.launchSummaries.length > 10){
                vm.showPagination = true;
            } else {
                vm.showPagination = true;
            }
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