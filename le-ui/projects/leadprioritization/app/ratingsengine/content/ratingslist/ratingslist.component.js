angular.module('lp.ratingsengine.ratingslist', [
    'mainApp.ratingsengine.deleteratingmodal'
])
.controller('RatingsEngineListController', function ($scope, $timeout, $element, $state, 
$stateParams, $filter, RatingList, RatingsEngineStore, RatingsEngineService, DeleteRatingModal) {

    var vm = this;
    angular.extend(vm, {
        ratings: RatingList || [],
        filteredItems: [],
        tileStates: {},
        query: '',
        loadingRatingChart: true,
        buckets: {},
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'created',
                items: [
                    { label: 'Last Data Refresh',   icon: 'numeric',    property: 'updated' },
                    { label: 'Creation Date',   icon: 'numeric',    property: 'created' },
                    { label: 'Rating Name',      icon: 'alpha',      property: 'displayName' }
                ]
            },
            filter: {
                label: 'Filter By',
                unfiltered: RatingList,
                filtered: RatingList,
                items: [
                    { label: "All", action: { }, total: vm.totalLength },
                    { label: "Active", action: { status: 'ACTIVE' }, total: vm.activeCount },
                    { label: "Inactive", action: { status: 'INACTIVE' }, total: vm.inactiveCount },
                ]
            }
        }
    });

    vm.init = function($q, $filter) {
        
        var checkLaunchState,
            arrayofIds = [];

        RatingsEngineStore.clear();

        angular.forEach(vm.ratings, function(rating) {
            
            var ratingId = rating.id;

            vm.tileStates[ratingId] = {
                showCustomMenu: false,
                editRating: false
            };
            arrayofIds.push(ratingId);

        });

        RatingsEngineStore.getRatingsChartData(arrayofIds).then(function(response){

            vm.loadingRatingChart = false;
            vm.buckets = response;

            angular.forEach(vm.ratings, function(rating) {

                rating.bucketInfo = vm.buckets.ratingEngineIdCoverageMap[rating.id];

                var bucketArray = (rating && rating.bucketInfo ? rating.bucketInfo.bucketCoverageCounts || [] : []);
                rating.tallestBarHeight = Math.max.apply(Math,bucketArray.map(function(o){return o.count;}))
            });

        });  

    }
    vm.init();

    vm.hasRules = function(rating) {
        return RatingsEngineStore.hasRules(rating);
    }

    vm.customMenuClick = function ($event, rating) {

        if ($event != null) {
            $event.stopPropagation();
        }

        var tileState = vm.tileStates[rating.id];
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

    vm.tileClick = function ($event, rating) {
        $event.preventDefault();
        // go to dashboard if there are rules in ratingModels
        if(RatingsEngineStore.hasRules(rating)) {
            $state.go('home.ratingsengine.dashboard', {rating_id: rating.id} );
        } else {
           $state.go('home.ratingsengine.wizard.segment', {rating_id: rating.id} ); 
        }
    };

    var oldRatingDisplayName = '';
    vm.editRatingClick = function($event, rating){
        $event.stopPropagation();

        oldRatingDisplayName = rating.displayName;

        var tileState = vm.tileStates[rating.id];
        tileState.showCustomMenu = !tileState.showCustomMenu;
        tileState.editRating = !tileState.editRating;
    };

    vm.cancelEditRatingClicked = function($event, rating) {
        $event.stopPropagation();

        rating.displayName = oldRatingDisplayName;
        oldRatingDisplayName = '';

        var tileState = vm.tileStates[rating.id];
        tileState.editRating = !tileState.editRating;
    };

    vm.editStatusClick = function($event, rating, disable){
        $event.stopPropagation();
        
        if(disable) {
            return false;
        }

        vm.saveInProgress = true;

        if(rating.status === 'ACTIVE'){
            var updatedRating = {
                id: rating.id,
                status: 'INACTIVE'   
            }
        } else {
            var updatedRating = {
                id: rating.id,
                status: 'ACTIVE'   
            }
        }

        updateRating(updatedRating);

    };

    vm.saveRatingClicked = function($event, rating) {
        $event.stopPropagation();

        vm.saveInProgress = true;
        oldRatingDisplayName = '';

        console.log(rating);

        var updatedRating = {
            id: rating.id,
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
                $state.go('home.ratingsengine.ratingslist', {}, { reload: true} );
            }, 100 );
        });

    }
});
