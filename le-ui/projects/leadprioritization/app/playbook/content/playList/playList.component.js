angular.module('lp.playbook.plays', [
    'mainApp.playbook.PlayListService',
    'mainApp.playbook.DeletePlayModal'
])
.controller('PlayListController', function ($scope, $element, $state, $stateParams,
    PlayList, DeletePlayModal, PlayListService) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        plays: PlayList,
        showCustomMenu: false,
        editPlay: false
    });

    vm.init = function($q) {
        console.log(vm.plays);
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

        if ($state.current.name == 'home.playbook') {
            $state.go('home.playbook', { reload: true } );
        } else {
            $state.go('home.playbook.wizard', {play: playName}, { reload: true } );
        }

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

        DeletePlayModal.show(play, !!vm.playName);

    };

    function updatePlay(play) {
        PlayListService.updatePlay(play).then(function(result) {

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
