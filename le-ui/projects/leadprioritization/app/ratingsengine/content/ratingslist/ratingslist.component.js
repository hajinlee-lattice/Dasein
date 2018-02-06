angular.module('lp.ratingsengine.ratingslist', [
    'mainApp.ratingsengine.deleteratingmodal',
    'mainApp.appCommon.directives.barchart'
])
.controller('RatingsEngineListController', function (
    $scope, $timeout, $element, $state, $stateParams, $filter, $interval, 
    RatingsEngineStore, RatingsEngineService, DeleteRatingModal
) {
    var vm = this;

    angular.extend(vm, {
        current: RatingsEngineStore.current,
        inEditing: {},
        query: '',
        ceil: window.Math.ceil,
        createRatingState: 'home.ratingsengine.ratingsenginetype',
        currentPage: 1,
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'updated',
                items: [
                    { label: 'Last Modified', icon: 'numeric', property: 'updated' },
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
                    { label: "Inactive", action: { status: 'INACTIVE' }, total: vm.inactiveCount }
                ]
            }
        }
    });

    vm.count = function(type) {
        return $filter('filter')(vm.current.ratings, { status: type }, true).length;
    }

    vm.init = function($q, $filter) {

        console.log(vm.current);

        RatingsEngineStore.clear();

        vm.totalLength = vm.count();
        vm.activeCount = vm.count('ACTIVE');
        vm.inactiveCount = vm.count('INACTIVE');

        angular.forEach(vm.current.ratings, function(rating) {
            if(vm.current.bucketCountMap[rating.id].bucketCoverageCounts.length === 0) {
                rating.hasBuckets = false;
            } else {
                rating.hasBuckets = true;
            }
        });
        
        $scope.$watch('vm.current.ratings', function() {
            vm.header.filter.unfiltered = vm.current.ratings;
        });
    }
    /**
     * if they decide they want to add sorting by account or contact counts uncomment this and add
     * { label: 'Accounts', icon: 'numeric', property: 'accountCount' },
     * { label: 'Contacts', icon: 'numeric', property: 'contactCount' }
     * to sort object above
     */
    // var checkForBuckets = $interval(function() {
    //     if(vm.current.bucketCountMap && vm.current.ratings) {
    //         angular.forEach(vm.current.ratings, function(rating, key) {
    //             rating.accountCount = vm.current.bucketCountMap[rating.id].accountCount || 0;
    //             rating.contactCount = vm.current.bucketCountMap[rating.id].contactCount || 0;
    //         })
    //         $interval.cancel(checkForBuckets);
    //     }
    // }, 1000);
    vm.init();

    vm.hasRules = function(rating) {
        return RatingsEngineStore.hasRules(rating);
    }

    vm.customMenuClick = function ($event, rating) {
        if ($event != null) {
            $event.stopPropagation();
        }

        var tileState = vm.current.tileStates[rating.id];
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
        
        var tileState = vm.current.tileStates[rating.id],
            url = 'home.ratingsengine.dashboard';

        if(tileState.editRating !== true){

            if (rating.type === 'AI_BASED') {
                url = 'home.ratingsengine.productpurchase.segment'
            } else if (rating.type === 'RULE_BASED') {
                url = RatingsEngineStore.hasRules(rating) 
                    ? 'home.ratingsengine.dashboard'
                    : 'home.ratingsengine.rulesprospects.segment';
            }

            $state.go(url, { rating_id: rating.id }); 
        }
    };

    vm.editRatingClick = function($event, rating) {
        $event.stopPropagation();
        vm.inEditing = angular.copy(rating);
        var tileState = vm.current.tileStates[rating.id];
        tileState.showCustomMenu = !tileState.showCustomMenu;
        tileState.editRating = !tileState.editRating;
    };

    vm.nameChanged = function(rating) {
        var tileState = vm.current.tileStates[rating.id];
        tileState.saveEnabled = rating.displayName.length > 0 && rating.displayName.trim() != '';
    };

    vm.cancelEditRatingClicked = function($event, rating) {
        $event.stopPropagation();
        rating.displayName = vm.inEditing.displayName;
        var tileState = vm.current.tileStates[rating.id];
        tileState.editRating = !tileState.editRating;
    };

    vm.editStatusClick = function($event, rating, disable){
        $event.stopPropagation();
        
        if (disable) {
            return false;
        }

        var newStatus = (rating.status === 'ACTIVE' ? 'INACTIVE' : 'ACTIVE');
        var updatedRating = {
                id: rating.id,
                status: newStatus,
            }
        updateRating(rating, updatedRating);
        RatingsEngineStore.setRatings(vm.current.ratings, true);

    };

    vm.saveRatingClicked = function($event, rating) {
        $event.stopPropagation();
        var updatedRating = {
            id: rating.id,
            displayName: rating.displayName
        }
        updateRating(rating, updatedRating);
    };


    vm.showDeleteRatingModalClick = function($event, rating){
        $event.preventDefault();
        $event.stopPropagation();

        DeleteRatingModal.show(rating);

    };

    function updateRating(rating, updatedRating) {
        vm.saveInProgress = true;
        RatingsEngineService.saveRating(updatedRating).then(function(result) {
            vm.saveInProgress = false;
            rating.status = result.status;
            vm.inEditing = {};
            vm.current.tileStates[rating.id].editRating = false;
            rating.displayName = result.displayName; //updates display name of rating; otherwise displays old name
        });
    }
});
