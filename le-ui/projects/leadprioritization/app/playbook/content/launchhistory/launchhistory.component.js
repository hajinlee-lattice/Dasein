angular.module('lp.playbook.dashboard.launchhistory', [])
.controller('PlaybookDashboardLaunchHistory', function(
    $scope, $state, $stateParams, $filter, $timeout, ResourceUtility, PlaybookWizardStore, LaunchHistoryData, LaunchHistoryCount
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.settings_form,
        current: PlaybookWizardStore.current,
        launches: LaunchHistoryData,
        launchesCount: LaunchHistoryCount,
        summaryData: {},
        launching: false,
        currentPage: 1,
        pagesize: 10,
        showPagination: false,
        orgId: '',
        externalSystemType: '',
        playName: '',
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

        console.log(vm.launches);
        console.log(vm.launchesCount);

        vm.noData = (vm.launchesCount === 0 && vm.orgId === '' && vm.externalSystemType === '' && vm.playName === '') ? true : false;

        vm.offset = (vm.currentPage - 1) * vm.pagesize;
        vm.updateLaunchData();
        vm.parseLaunchData();
        vm.updateFilterData();

    };


    // Set sort
    vm.sort = function(header) {
        vm.sortBy = header;

        vm.currentPage = 1;
        vm.offset = 0;
        vm.updateLaunchData();
    }

    // Set play name
    vm.playSelectChange = function(play){
        if(play === undefined || play.length == 0){
            vm.playName = null;
        } else {
            vm.playName = play[0].name;
        }

        vm.currentPage = 1;
        vm.offset = 0;
        vm.updateLaunchData();
    };

    // Get data
    vm.updateLaunchData = function() {

        var params = {
                playName: vm.playName || $stateParams.play_name,
                sortby: vm.sortBy,
                descending: vm.sortDesc,
                offset: vm.offset,
                max: 10,
                orgId: vm.orgId,
                externalSysType: vm.externalSystemType
            },
            countParams = {
                playName: vm.playName || $stateParams.play_name,
                offset: vm.offset,
                startTimestamp: 0,
                orgId: vm.orgId,
                externalSysType: vm.externalSystemType
            };

        PlaybookWizardStore.getPlayLaunches(params).then(function(result){
            vm.launches = result;
            $timeout(function(){
                vm.parseLaunchData();
            }, 1000);
            
        });
        PlaybookWizardStore.getPlayLaunchCount(countParams).then(function(result){
            vm.launchesCount = result;
            if(result > vm.pagesize){
                vm.showPagination = true;
            } else {
                vm.showPagination = false;
            }
        });

    };

    vm.parseLaunchData = function() {

        vm.noFilteredData = (vm.launchesCount === 0 && (vm.orgId !== '' || vm.externalSystemType !== '' || vm.playName !== '')) ? true : false;

        vm.defaultPlayLaunchList = angular.copy(vm.launches.uniquePlaysWithLaunches);
        vm.defaultPlayLaunchList.unshift({playName: null, displayName: 'All Launched Plays'});

        vm.header.filter.filtered = vm.defaultPlayLaunchList;
        vm.header.filter.unfiltered = vm.defaultPlayLaunchList;

        vm.allPlaysHistory = ($state.current.name === 'home.playbook.plays.launchhistory') ? true : false;

        var launchSummaries = vm.launches.launchSummaries;
        for(var i = 0; i < launchSummaries.length; i++) {
            if (launchSummaries[i].launchState == 'Launching') {
                vm.launching = true;
                break;
            }
        }

        // Display correct cumulative stats in summary area
        var stats = vm.launches.cumulativeStats;
        vm.summaryData = {
            selectedTargets: stats.selectedTargets,
            suppressed: stats.suppressed,
            errors: stats.errors,
            recommendationsLaunched: stats.recommendationsLaunched,
            contactsWithinRecommendations: stats.contactsWithinRecommendations
        }
    }

    // Watch for change in pagination
    $scope.$watch('vm.currentPage', function(newValue, oldValue) {
        vm.loading = true;
        if (newValue != oldValue) {
            vm.offset = (vm.currentPage - 1) * vm.pagesize,
            vm.updateLaunchData();
        }
    });

    vm.filterChange = function(org) {
        var orgData = org[1];

        vm.orgId = orgData.destinationOrgId;
        vm.externalSystemType = orgData.externalSystemType;

        vm.currentPage = 1;
        vm.offset = 0;
        vm.updateLaunchData();
    }

    // Create list of items for filter
    vm.updateFilterData = function() {
        vm.header.filter.items = [
            { 
                label: "All", 
                action: {
                    destinationOrgId: ''
                },
                total: vm.launchesCount
            }
        ];

        angular.forEach(vm.launches.uniqueLookupIdMapping, function(value, key) {
            angular.forEach(value, function(val, index) {
   
                vm.header.filter.items.push({ 
                    label: val.orgName,
                    data: {
                        orgName: val.orgName,
                        externalSystemType: val.externalSystemType,
                        destinationOrgId: val.orgId
                    }, 
                    action: {
                        destinationOrgId: val.orgId
                    }
                });

            });
        });

    }

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