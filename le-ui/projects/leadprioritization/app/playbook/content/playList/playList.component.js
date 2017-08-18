angular.module('lp.playbook.plays', [
    'mainApp.playbook.content.playList.deletePlayModal'
])
.controller('PlayListController', function ($scope, $timeout, $element, $state, 
$stateParams, PlayList, PlaybookWizardService, PlaybookWizardStore, DeletePlayModal) {

    var vm = this;
    angular.extend(vm, {
        plays: PlayList || [],
        filteredItems: [],
        totalLength: PlayList.length,
        tileStates: {},
        query: '',
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'timeStamp',
                items: [
                    { label: 'Modified Date',   icon: 'numeric',    property: 'lastUpdatedTimestamp' },
                    { label: 'Creation Date',   icon: 'numeric',    property: 'timeStamp' },
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

    vm.init = function($q) {


        // console.log(vm.plays);

        PlaybookWizardStore.clear();
        angular.forEach(PlayList, function(play) {
            vm.tileStates[play.name] = {
                showCustomMenu: false,
                editSegment: false,
                launching: false
            };

            if(play.segment != null) {
                play.hasSegment = true;
            }

        });

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

    vm.tileClick = function ($event, playName) {
        $event.preventDefault();
        //$state.go('home.playbook.wizard.settings', {play_name: playName} );
        $state.go('home.playbook.dashboard', {play_name: playName} );
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

        $event.stopPropagation();

        var tileState = vm.tileStates[play.name];
        tileState.launching = !tileState.launching;

        PlaybookWizardStore.launchPlay(play).then(function(data) {
            $state.go('home.playbook.dashboard.launch_job', {play_name: play.name, applicationId: data.applicationId});
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
});
