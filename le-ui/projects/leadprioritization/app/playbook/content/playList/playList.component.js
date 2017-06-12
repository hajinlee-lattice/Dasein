angular.module('lp.playbook.plays', [
    'mainApp.playbook.content.playList.deletePlayModal'
])
.controller('PlayListController', function ($scope, $element, $state, 
$stateParams, PlayList, PlaybookWizardService, DeletePlayModal) {

    var vm = this;
    angular.extend(vm, {
        plays: PlayList,
        totalLength: PlayList.length,
        tileStates: {},
        filteredItems: [],
        query: '',
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'TimeStamp',
                items: [
                    { label: 'Creation Date',   icon: 'numeric',    property: 'timestamp' },
                    { label: 'Play Name',      icon: 'alpha',      property: 'dislay_name' }
                ]
            }
        }
    });

    vm.init = function($q) {
        PlayList.forEach(function(play) {
            vm.tileStates[play.name] = {
                showCustomMenu: false,
                editSegment: false
            };
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
        $state.go('home.playbook.wizard.settings', {play_name: playName} );
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

        updatePlay(play);
    };


    vm.showDeletePlayModalClick = function($event, play){

        $event.preventDefault();
        $event.stopPropagation();

        DeletePlayModal.show(play);

    };

    function updatePlay(play) {
        PlaybookWizardService.savePlay(play).then(function(result) {

            var errorMsg = result.errorMsg;

            if (result.success) {
                if ($state.current.name == 'home.playbook') {
                    $state.go('home.playbook', {}, { reload: true});
                } else {
                    $state.go('home.playbook.wizard', {}, { reload: true });
                }
            } else {
                vm.saveInProgress = false;
                vm.addPlayErrorMessage = errorMsg;
                vm.showAddPlayError = true;
            }
        });

    }
});
