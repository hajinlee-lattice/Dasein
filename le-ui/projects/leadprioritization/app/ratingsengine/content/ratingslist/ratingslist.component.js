angular.module('lp.ratingsengine.ratingslist', [
    'mainApp.ratingsengine.deleteratingmodal'
])
.controller('RatingsEngineListController', function ($scope, $timeout, $element, $state, 
$stateParams, RatingList, RatingsEngineStore, RatingsEngineService, DeleteRatingModal) {

    var vm = this;
    angular.extend(vm, {
        ratings: RatingList || [],
        filteredItems: [],
        tileStates: {},
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
                    { label: 'Rating Name',      icon: 'alpha',      property: 'displayName' }
                ]
            },
            filter: {
                label: 'Filter By',
                unfiltered: RatingList,
                filtered: RatingList,
                items: [
                    { label: "All", action: { }, total: vm.totalLength }
                    // { 
                    //     label: "Draft", 
                    //     action: { 
                    //         launchHistory: {playLaunch: null},
                    //         segment: null
                    //     }, 
                    //     total: ''
                    // }
                ]
            }
        }
    });

    vm.init = function($q) {


        // console.log(vm.ratings);
        var checkLaunchState;
        RatingsEngineStore.clear();
        angular.forEach(RatingList, function(rating) {

            vm.tileStates[rating.name] = {
                showCustomMenu: false
            };


        });

    }
    vm.init();

    vm.customMenuClick = function ($event, rating) {

        if ($event != null) {
            $event.stopPropagation();
        }

        var tileState = vm.tileStates[rating.name];
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


    vm.tileClick = function ($event, ratingId) {
        $event.preventDefault();
        $state.go('home.ratingsengine.dashboard', {rating_id: ratingId} );
    };


    var oldRatingDisplayName = '';
    vm.editRatingClick = function($event, rating){
        $event.stopPropagation();

        oldRatingDisplayName = rating.displayName;

        var tileState = vm.tileStates[rating.name];
        tileState.showCustomMenu = !tileState.showCustomMenu;
        tileState.editRating = !tileState.editRating;
    };

    vm.cancelEditRatingClicked = function($event, rating) {
        $event.stopPropagation();

        rating.displayName = oldRatingDisplayName;
        oldRatingDisplayName = '';

        var tileState = vm.tileStates[rating.name];
        tileState.editRating = !tileState.editRating;
    };

    vm.saveRatingClicked = function($event, rating) {
        $event.stopPropagation();

        vm.saveInProgress = true;
        oldRatingDisplayName = '';

        var updatedRating = {
            name: rating.name,
            displayName: rating.displayName
        }

        updateRating(updatedRating);
    };


    vm.showDeleteRatingModalClick = function($event, rating){

        $event.preventDefault();
        $event.stopPropagation();

        DeleteRatingModal.show(rating);

    };

    function updateRating(updatedRating) {

        RatingsEngineService.saveRating(updatedRating).then(function(result) {
            vm.saveInProgress = true;
            $timeout( function(){
                $state.go('home.ratingsengine.ratings', {}, { reload: true} );
            }, 100 );
        });

    }
});
