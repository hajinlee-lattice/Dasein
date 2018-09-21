angular.module('lp.playbook.plays', [
    'mainApp.playbook.content.playlist.modals.deletePlayModal',
    'mainApp.appCommon.services.FilterService'
])
.controller('PlayListController', function ($scope, $timeout, $element, $state, $stateParams, $interval, 
    PlaybookWizardService, PlaybookWizardStore, TimestampIntervalUtility, NumberUtility, DeletePlayModal, QueryStore) {

    var vm = this,
        onpage = true,
        checkLaunchState;

    angular.extend(vm, {
        current: PlaybookWizardStore.current,
        inEditing: {},
        TimestampIntervalUtility: TimestampIntervalUtility,
        NumberUtility: NumberUtility,
        lockLaunching: false,
        ceil: window.Math.ceil,
        query: '',
        currentPage: 1,
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'updated',
                items: [
                    { label: 'Modified Date',   icon: 'numeric',    property: 'updated' },
                    { label: 'Creation Date',   icon: 'numeric',    property: 'created' },
                    { label: 'Campaign Name',      icon: 'alpha',      property: 'displayName' }
                ]
            },
            filter: {
                label: 'Filter By',
                value: {},
                items: [
                    { label: "All", action: { }, total: PlaybookWizardStore.current.plays.length },
                    {
                       label: "Launched", 
                        action: { 
                            launchHistory: {playLaunch: []}
                        }, 
                        total: ''
                    }
                ]
            }
        },
        barChartConfig: PlaybookWizardStore.barChartConfig,
        barChartLiftConfig: PlaybookWizardStore.barChartLiftConfig
    });

    vm.init = function($q) {
        // console.log(vm.current.plays);
        PlaybookWizardStore.clear();

        $scope.$watch('vm.current.plays', function() {
            vm.header.filter.filtered = vm.current.plays;
            vm.header.filter.unfiltered = vm.current.plays;

            angular.forEach(vm.current.plays, function(play, key) {

                if(play.ratingEngine.type === 'CROSS_SELL' && play.ratingEngine.advancedRatingConfig) {
                    play.ratingEngine.tileClass = play.ratingEngine.advancedRatingConfig.cross_sell.modelingStrategy;
                } else {
                    play.ratingEngine.tileClass = play.ratingEngine.type;
                }

                if(play.ratingEngine.type === 'CROSS_SELL' || play.ratingEngine.type === 'CUSTOM_EVENT') {
                    play.ratingEngine.chartConfig = vm.barChartLiftConfig;
                } else {
                    play.ratingEngine.chartConfig = vm.barChartConfig;
                }        

                var newBucketMetadata = [];

                if(play.ratingEngine.bucketMetadata && play.ratingEngine.bucketMetadata.length > 0) {
                    angular.forEach(play.ratingEngine.bucketMetadata, function(rating, key) {
                        rating.lift = (Math.round( rating.lift * 10) / 10).toString();
                        newBucketMetadata.push(rating);
                    });
                }
                play.ratingEngine.newBucketMetadata = newBucketMetadata;

            });
        });

    }

    vm.init();

    vm.sumValuesOfObject = function(object) {
        var sum = 0;
        for(var i=0; i < Object.values(object).length; i++){
            sum += Object.values(object)[i];
        }
        return sum;
    }

    vm.customMenuClick = function ($event, play) {

        if ($event != null) {
            $event.stopPropagation();
        }

        var tileState = vm.current.tileStates[play.name];
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
        PlaybookWizardStore.setPlay(play);

        if(launchedStatus.hasLaunched) {
            $state.go('home.playbook.dashboard', {play_name: play.name} );
        } else if (play.ratingEngine && play.ratingEngine.id) {
            $state.go('home.playbook.dashboard', {play_name: play.name} );
            //$state.go('home.playbook.create.rating', { play_name: play.name, rating_id: play.ratingEngine.id } );
        }
    };

    var oldPlayDisplayName = '';
    vm.editPlayClick = function($event, play){
        $event.stopPropagation();
        vm.inEditing = angular.copy(play);
        var tileState = vm.current.tileStates[play.name];
        tileState.showCustomMenu = !tileState.showCustomMenu;
        tileState.editPlay = !tileState.editPlay;
    };

    vm.nameChanged = function(play){
        var tileState = vm.current.tileStates[play.name];
        if(play.displayName.length > 0) {
            tileState.saveEnabled = true;
        } else {
            tileState.saveEnabled = false;
        }
    };

    vm.cancelEditPlayClicked = function($event, play) {
        $event.stopPropagation();

        var tileState = vm.current.tileStates[play.name];
        tileState.editPlay = !tileState.editPlay;
        play.displayName = vm.inEditing.displayName || play.displayName;
        vm.inEditing = {};
    };

    vm.savePlayClicked = function($event, play) {
     
        var updatedPlayName = play.displayName;
        var updatedPlay = {
            name: play.name,
            displayName: updatedPlayName //save updated rating name
        }

        vm.saveInProgress = true;
        updatePlay(updatedPlay);
    };

    vm.launchPlay = function($event, play) {
        vm.lockLaunching = false;
        $event.stopPropagation();

        var tileState = vm.current.tileStates[play.name];
        tileState.launching = !tileState.launching;

        var opts = play.launchHistory.mostRecentLaunch;
        PlaybookWizardStore.launchPlay(play, opts).then(function(data) {
            vm.lockLaunching = false;
        });
    }

    vm.showDeletePlayModalClick = function($event, play){

        $event.preventDefault();
        $event.stopPropagation();

        DeletePlayModal.show(play);

    };

    function updatePlay(updatedPlay) {
        PlaybookWizardService.savePlay(updatedPlay).then(function(result) {
            vm.saveInProgress = false;
            vm.current.tileStates[updatedPlay.name].editPlay = false;
            vm.inEditing = {};
        });

        // PlaybookWizardService.savePlay(updatedPlay).then(function(result) {
        //     vm.saveInProgress = true;
        //     $timeout( function(){
        //         $state.go('home.playbook.plays', {}, { reload: true} );
        //     }, 100 );
        // });

    }

    $scope.$on('$destroy', function() {
        onpage = false;
        PlaybookWizardStore.cancelCheckLunch();
    });

    
});
