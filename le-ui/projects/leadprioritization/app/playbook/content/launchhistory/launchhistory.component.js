angular.module('lp.playbook.dashboard.launchhistory', [])
.controller('PlaybookDashboardLaunchHistory', function(
    $scope, $state, $stateParams, $timeout, ResourceUtility, PlaybookWizardStore, LaunchHistoryData, LaunchHistoryCount
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.settings_form,
        current: PlaybookWizardStore.current,
        launches: LaunchHistoryData,
        launchesCount: LaunchHistoryCount,
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

        if(vm.launchesCount > 10){
            vm.showPagination = true;
        } else {
            vm.showPagination = false;
        }

        vm.defaultPlayLaunchList = angular.copy(vm.launches.uniquePlaysWithLaunches);
        vm.defaultPlayLaunchList.unshift({playName: null, displayName: 'All Launched Plays'});

        if($state.current.name === 'home.playbook.plays.launchhistory'){
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
    };

    vm.count = function(type, current) {
        var filter = current
            ? { launchHistory: { playLaunch: { launchState: type } } }
            : { launchHistory: { mostRecentLaunch: { launchState: type } } };
        
        return ($filter('filter')(vm.current.plays, filter, true) || []).length;
    }

    function updatePage() {
        var offset = (vm.current - 1) * vm.pagesize,
            params = {
                playName: '',
                sortBy: 'created',
                descending: true,
                offset: offset,
                startTimestamp: vm.launches.launchSummaries[vm.launches.launchSummaries.length - 1].launchTime
            };

        PlaybookWizardStore.getPlayLaunches(params).then(function(result){
            vm.launches = result;
        });
    }

    $scope.$watch('vm.current', function(newValue, oldValue) {
        vm.loading = true;
        if (newValue != oldValue) {
            updatePage();    
        }
    });

    vm.sort = function(header) {
        vm.sortBy = header;
        var params = {
            playName: $stateParams.play_name,
            sortby: header,
            descending: vm.sortDesc,
            offset: 0
        };
        PlaybookWizardStore.getPlayLaunches(params).then(function(result){
            vm.launches = result;
        });
    }

    vm.playSelectChange = function(play){
        var playName = null;

        if(play === undefined || play.length == 0){
            playName = null;
        } else {
            playName = play[0].name;
        }

        var dataParams = {
                playName: playName,
                offset: 0
            },
            countParams = {
                playName: playName,
                offset: 0,
                startTimestamp: 0
            };

        PlaybookWizardStore.getPlayLaunches(dataParams).then(function(result){
            vm.launches = result;
        });
        PlaybookWizardStore.getPlayLaunchCount(countParams).then(function(result){
            if(result > 10){
                vm.showPagination = true;
            } else {
                vm.showPagination = false;
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