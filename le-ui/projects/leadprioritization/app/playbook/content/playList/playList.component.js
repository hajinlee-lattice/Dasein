angular.module('lp.playbook.plays', [
    'mainApp.playbook.content.playList.deletePlayModal'
])
.controller('PlayListController', function ($scope, $timeout, $element, $state, 
$stateParams, $interval, PlayList, PlaybookWizardService, PlaybookWizardStore, TimestampIntervalUtility, NumberUtility, DeletePlayModal) {

    var vm = this,
        onpage = true,
        checkLaunchState;
    angular.extend(vm, {
        plays: PlayList || [],
        filteredItems: [],
        totalLength: PlayList.length,
        tileStates: {},
        TimestampIntervalUtility: TimestampIntervalUtility,
        NumberUtility: NumberUtility,
        lockLaunching: false,
        query: '',
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'created',
                items: [
                    { label: 'Modified Date',   icon: 'numeric',    property: 'updated' },
                    { label: 'Creation Date',   icon: 'numeric',    property: 'created' },
                    { label: 'Play Name',      icon: 'alpha',      property: 'displayName' }
                ]
            },
            filter: {
                label: 'Filter By',
                unfiltered: PlayList,
                filtered: PlayList,
                items: [
                    { label: "All", action: { }, total: vm.totalLength },
                    { 
                        label: "Draft", 
                        action: { 
                            launchHistory: {playLaunch: null},
                            segment: null
                        }, 
                        total: ''
                    },
                    { 
                        label: "Ready to Launch", 
                        action: { 
                            launchHistory: {playLaunch: null},
                            hasSegment: true
                        }, 
                        total: '' 
                    },
                    {
                       label: "Launched", 
                        action: { 
                            launchHistory: {playLaunch: []}
                        }, 
                        total: ''  
                    }
                ]
            }
        }
    });

    var checkLaunchState = {};
    vm.checkLaunchStateInterval = function(play){
        checkLaunchState[play.name] = $interval(function() {
            PlaybookWizardStore.getPlayLaunches(play.name).then(function(result) {
                if(result.errorCode) {
                    $interval.cancel(checkLaunchState[play.name]);
                } else if(result && result[0]) {
                    if(result[0].launchState === 'Launched' || result[0].launchState === 'Failed') {
                        $interval.cancel(checkLaunchState[play.name]);
                        play.launchHistory.mostRecentLaunch.launchState = result[0].launchState;
                        vm.tileStates[play.name].launching == false;
                    }
                } 

                // var result = results[0];
                // vm.launchHistory = results;
                // vm.launchedState = (result && result.launchState ? result.launchState : null);

                // vm.launchButton = PlaybookWizardStore.launchButton(play, vm.launchedState);
                // if(vm.launchedState === 'Launched' || vm.launchedState === 'Failed') {
                //     $interval.cancel(checkLaunchState);
                //     vm.showLaunchSpinner = false;
                // }

            });
        }, 10 * 1000);

    };

    vm.init = function($q) {
        angular.forEach(PlayList, function(play) {

            vm.tileStates[play.name] = {
                showCustomMenu: false,
                editSegment: false,
                launching: false
            };

            if(play.launchHistory.mostRecentLaunch != null && play.launchHistory.mostRecentLaunch.launchState === 'Launching'){
                vm.tileStates[play.name].launching = true;
                vm.checkLaunchStateInterval(play);
            }

            if(play.segment != null) {
                play.hasSegment = true;
            };
        });
        PlaybookWizardStore.clear();
    }

    vm.init();

    vm.customMenuClick = function ($event, play) {

        if ($event != null) {
            $event.stopPropagation();
        }

        var tileState = vm.tileStates[play.name];
        tileState.showCustomMenu = !tileState.showCustomMenu

        if (tileState.showCustomMenu) {
            $(document).bind('click', function(event){
                var isClickedElementChildOfPopup = $element
                    .find(event.target)
                    .length > 0;

                if (isClickedElementChildOfPopup)
                    return;

                $scope.$apply(function(){
                    tileState.showCustomMenu = false;
                    $(document).unbind(event);
                });
            });
        }
    };

    vm.tileClick = function ($event, play) {
        $event.preventDefault();
        var launchedStatus = PlaybookWizardStore.getLaunchedStatus(play);
        if(launchedStatus.hasLaunched) {
            $state.go('home.playbook.dashboard', {play_name: play.name} );
        } else {
            $state.go('home.playbook.wizard.rating', {play_name: play.name} );
        }
    };

    var oldPlayDisplayName = '';
    vm.editPlayClick = function($event, play){
        $event.stopPropagation();

        oldPlayDisplayName = play.displayName;

        var tileState = vm.tileStates[play.name];
        tileState.showCustomMenu = !tileState.showCustomMenu;
        tileState.editPlay = !tileState.editPlay;
    };

    vm.cancelEditPlayClicked = function($event, play) {
        $event.stopPropagation();

        play.displayName = oldPlayDisplayName;
        oldPlayDisplayName = '';

        var tileState = vm.tileStates[play.name];
        tileState.editPlay = !tileState.editPlay;
    };

    vm.savePlayClicked = function($event, play) {
        $event.stopPropagation();

        vm.saveInProgress = true;
        oldPlayDisplayName = '';

        var updatedPlay = {
            name: play.name,
            displayName: play.displayName
        }

        updatePlay(updatedPlay);
    };

    vm.launchPlay = function($event, play) {
        vm.lockLaunching = false;
        $event.stopPropagation();

        var tileState = vm.tileStates[play.name];
        tileState.launching = !tileState.launching;

        PlaybookWizardStore.launchPlay(play).then(function(data) {
            vm.lockLaunching = false;
            //PlaybookWizardStore.clear();
            //$state.go('home.playbook.dashboard.launch_job', {play_name: play.name, applicationId: data.applicationId});
        });

    }

    vm.showDeletePlayModalClick = function($event, play){

        $event.preventDefault();
        $event.stopPropagation();

        DeletePlayModal.show(play);

    };

    function updatePlay(updatedPlay) {

        PlaybookWizardService.savePlay(updatedPlay).then(function(result) {
            vm.saveInProgress = true;
            $timeout( function(){
                $state.go('home.playbook.plays', {}, { reload: true} );
            }, 100 );
        });

    }

    $scope.$on('$destroy', function() {
        onpage = false;
        for(var i in checkLaunchState) {
            $interval.cancel(checkLaunchState[i]);
        }
        checkLaunchState = {};
    });

    
});
