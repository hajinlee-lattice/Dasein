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
        currentPage: 1,
        pagesize: 10,
        showPagination: false,
        playName: null,
        sortBy: 'created',
        sortDesc: true,
        header: {
            filter: {
                label: 'Filter By',
                value: {}
            }
        }
    });

    vm.init = function() {

        // console.log(vm.stored);
        console.log(vm.launches);

        vm.updateFilterData();

        vm.defaultPlayLaunchList = angular.copy(vm.launches.uniquePlaysWithLaunches);
        vm.defaultPlayLaunchList.unshift({playName: null, displayName: 'All Launched Plays'});

        vm.header.filter.filtered = vm.defaultPlayLaunchList;
        vm.header.filter.unfiltered = vm.defaultPlayLaunchList;

        vm.showPagination = (vm.launchesCount > vm.pagesize) ? true : false;
        vm.allPlaysHistory = ($state.current.name === 'home.playbook.plays.launchhistory') ? true : false;

        var launchSummaries = vm.launches.launchSummaries;
        for(var i = 0; i < launchSummaries.length; i++) {
            if (launchSummaries[i].launchState == 'Launching') {
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

    vm.updateFilterData = function() {
        vm.header.filter.items = [
            { 
                label: "All", 
                action: {}, 
                total:  vm.launchesCount
            }
        ];

        angular.forEach(vm.launches.uniqueLookupIdMapping, function(value, key) {
            angular.forEach(value, function(val, index) {
   
                vm.header.filter.items.push({ 
                    label: val.orgName, 
                    action: {
                        destinationOrgId: val.orgId
                    }
                });

            });
        });
    }

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

        console.log(vm.playName, $stateParams.play_name);

        var params = {
                playName: vm.playName || $stateParams.play_name,
                sortby: header,
                descending: vm.sortDesc,
                offset: 0
            },
            countParams = {
                playName: vm.playName,
                offset: 0,
                startTimestamp: 0
            };

        PlaybookWizardStore.getPlayLaunches(params).then(function(result){
            vm.launches = result;
        });

        // PlaybookWizardStore.getPlayLaunchCount(countParams).then(function(result){
        //     if(result > vm.pagesize){
        //         vm.showPagination = true;
        //     } else {
        //         vm.showPagination = false;
        //     }
        // });
    }

    vm.playSelectChange = function(play){

        if(play === undefined || play.length == 0){
            vm.playName = null;
        } else {
            vm.playName = play[0].name;
        }

        var dataParams = {
                playName: vm.playName,
                offset: 0
            },
            countParams = {
                playName: vm.playName,
                offset: 0,
                startTimestamp: 0
            };

        PlaybookWizardStore.getPlayLaunches(dataParams).then(function(result){
            vm.launches = result;
        });
        PlaybookWizardStore.getPlayLaunchCount(countParams).then(function(result){
            vm.launchesCount = result;
            vm.updateFilterData();
            if(result > vm.pagesize){
                vm.showPagination = true;
            } else {
                vm.showPagination = false;
            }
        });
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