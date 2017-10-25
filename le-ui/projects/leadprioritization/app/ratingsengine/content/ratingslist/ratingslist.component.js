angular.module('lp.ratingsengine.ratingslist', [
    'mainApp.ratingsengine.deleteratingmodal'
])
.controller('RatingsEngineListController', function (
    $scope, $timeout, $element, $state, $stateParams, $filter, 
    RatingsEngineStore, RatingsEngineService, DeleteRatingModal
) {
    var vm = this;
    angular.extend(vm, {
        current: RatingsEngineStore.current,
        filteredItems: [],
        tileStates: {},
        query: '',
        ceil: window.Math.ceil,
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'created',
                items: [
                    { label: 'Last Data Refresh', icon: 'numeric', property: 'updated' },
                    { label: 'Creation Date', icon: 'numeric', property: 'created' },
                    { label: 'Rating Name', icon: 'alpha', property: 'displayName' }
                ]
            },
            filter: {
                label: 'Filter By',
                value: {},
                items: [
                    { label: "All", action: {}, total: vm.totalLength },
                    { label: "Active", action: { status: 'ACTIVE' }, total: vm.activeCount },
                    { label: "Inactive", action: { status: 'INACTIVE' }, total: vm.inactiveCount },
                ]
            }
        }
    });

    vm.init = function($q, $filter) {
        RatingsEngineStore.clear();

        vm.header.filter.filtered = vm.current.ratings;
        vm.header.filter.unfiltered = vm.current.ratings;

        angular.forEach(vm.current.ratings, function(rating) {
            vm.tileStates[rating.id] = {
                showCustomMenu: false,
                editRating: false,
                saveEnabled: false
            };
        });
    }

    vm.init();

    vm.getTallestBarHeight = function(bucket, rating) {
        var bucketArray = vm.current.bucketCountMap[rating.id].bucketCoverageCounts;

        return Math.max.apply(Math, bucketArray.map(function(bkt) { 
            return bkt.count; 
        }))
    }

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
        var url = RatingsEngineStore.hasRules(rating) 
            ? 'home.ratingsengine.dashboard'
            : 'home.ratingsengine.wizard.segment';

        $state.go(url, { rating_id: rating.id }); 
    };

    var oldRatingDisplayName = '';
    vm.editRatingClick = function($event, rating) {
        $event.stopPropagation();

        oldRatingDisplayName = rating.displayName;

        var tileState = vm.tileStates[rating.id];
        tileState.showCustomMenu = !tileState.showCustomMenu;
        tileState.editRating = !tileState.editRating;
    };

    vm.nameChanged = function(rating) {
        var tileState = vm.tileStates[rating.id];
        
        tileState.saveEnabled = !!(rating.displayName.length > 0);
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
        
        if (disable) {
            return false;
        }

        vm.saveInProgress = true;

        var newStatus = (rating.status === 'ACTIVE' ? 'INACTIVE' : 'ACTIVE');

        updateRating({
            id: rating.id,
            status: newStatus 
        });

        rating.status = newStatus;
    };

    vm.saveRatingClicked = function($event, rating) {
        $event.stopPropagation();

        oldRatingDisplayName = '';

        var tileState = vm.tileStates[rating.id];
        var updatedRating = {
            id: rating.id,
            displayName: rating.displayName
        }

        vm.saveInProgress = true;
        tileState.editRating = false;

        updateRating(updatedRating);
    };


    vm.showDeleteRatingModalClick = function($event, rating){
        $event.preventDefault();
        $event.stopPropagation();

        DeleteRatingModal.show(rating);
    };

    function updateRating(updatedRating) {
        RatingsEngineService.saveRating(updatedRating).then(function(result) {
            vm.saveInProgress = false;
        });
    }
});
