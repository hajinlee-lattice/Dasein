angular.module('lp.playbook.plays', [
    'mainApp.playbook.content.playList.deletePlayModal'
])
.controller('PlayListController', function ($scope, $timeout, $element, $state, 
$stateParams, PlayList, PlaybookWizardService, DeletePlayModal) {

    var vm = this;
    angular.extend(vm, {
        plays: PlayList || [],
        totalLength: PlayList.length,
        tileStates: {},
        filteredItems: [],
        query: '',
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'timestamp',
                items: [
                    { label: 'Creation Date',   icon: 'numeric',    property: 'timestamp' },
                    { label: 'Play Name',      icon: 'alpha',      property: 'dislay_name' }
                ]
            },
            filter: {
                label: 'Filter By',
                unfiltered: PlayList,
                filtered: PlayList,
                items: [
                    { label: "All", action: { }, total: 278 }
                ]
            }
        }
    });

    vm.init = function($q) {
        angular.forEach(PlayList, function(play) {
            vm.tileStates[play.name] = {
                showCustomMenu: false,
                editSegment: false
            };
        });
        // console.log(vm.plays);
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
    var oldPlayDescription = '';
    vm.editPlayClick = function($event, play){
        $event.stopPropagation();

        oldPlayDisplayName = play.display_name;
        oldPlayDescription = play.description;

        var tileState = vm.tileStates[play.name];
        tileState.showCustomMenu = !tileState.showCustomMenu;
        tileState.editPlay = !tileState.editPlay;
    };

    vm.cancelEditPlayClicked = function($event, play) {
        $event.stopPropagation();

        play.display_name = oldPlayDisplayName;
        play.description = oldPlayDescription;
        oldPlayDisplayName = '';
        oldPlayDescription = '';

        var tileState = vm.tileStates[play.name];
        tileState.editPlay = !tileState.editPlay;
    };

    vm.savePlayClicked = function($event, play) {

        $event.stopPropagation();

        vm.saveInProgress = true;
        oldPlayDisplayName = '';
        oldPlayDescription = '';

        var updatedPlay = {
            name: play.name,
            display_name: play.display_name,
            description: play.description
        }

        updatePlay(updatedPlay);
    };


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
