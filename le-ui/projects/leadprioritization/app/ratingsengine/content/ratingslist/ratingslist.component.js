angular.module('lp.ratingsengine.ratingslist', [
    'mainApp.ratingsengine.deleteratingmodal',
    'mainApp.appCommon.directives.barchart',
    'mainApp.core.utilities.NavUtility'
])
.controller('RatingsEngineListController', function (
    $scope, $timeout, $location, $element, $state, $stateParams, $filter, $interval, $rootScope,
    RatingsEngineStore, RatingsEngineService, DeleteRatingModal, NavUtility, StateHistory, JobsStore
) {
    var vm = this;

    angular.extend(vm, {
        current: RatingsEngineStore.current,
        inEditing: {},
        isRatingsSet: true,
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
                    { label: 'Model Name', icon: 'alpha', property: 'displayName' }
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
        },
        barChartConfig: {
            'data': {
                'tosort': true,
                'sortBy': '-num_leads',
                'trim': true,
                'top': 5,
            },
            'chart': {
                'header':'Value',
                'emptymsg': '',
                'usecolor': true,
                'color': '#e8e8e8',
                'mousehover': false,
                'type': 'integer',
                'showstatcount': false,
                'maxVLines': 3,
                'showVLines': false,
            },
            'vlines': {
                'suffix': ''
            },
            'columns': [{
                'field': 'num_leads',
                'label': 'Records',
                'type': 'number',
                'chart': true,
            }]
        },
        barChartLiftConfig: {
            'data': {
                'tosort': true,
                'sortBy': 'bucket_name',
                'trim': true,
                'top': 5,
            },
            'chart': {
                'header':'Value',
                'emptymsg': '',
                'usecolor': true,
                'color': '#e8e8e8',
                'mousehover': false,
                'type': 'decimal',
                'showstatcount': false,
                'maxVLines': 3,
                'showVLines': true,
            },
            'vlines': {
                'suffix': 'x'
            },
            'columns': [{
                    'field': 'lift',
                    'label': 'Lift',
                    'type': 'string',
                    'suffix': 'x',
                    'chart': true
                }
            ]
        }
    });

    vm.count = function(type) {
        return $filter('filter')(vm.current.ratings, { status: type }, true).length;
    }

    vm.init = function($q, $filter) {

        var arr = vm.current.ratings;

        RatingsEngineStore.clear();

        vm.totalLength = vm.count();
        vm.activeCount = vm.count('ACTIVE');
        vm.inactiveCount = vm.count('INACTIVE');

        var referringRoute = StateHistory.lastFrom().name,
            lastRouteContainsSegmentOrAttributes = (referringRoute.split('.').indexOf("products") > -1 || referringRoute.split('.').indexOf("attributes") > -1);
        
        $scope.$watch('vm.current.ratings', function() {
            if(lastRouteContainsSegmentOrAttributes){
                vm.isRatingsSet = RatingsEngineStore.ratingsSet;
            };
            vm.header.filter.unfiltered = vm.current.ratings;

            angular.forEach(vm.current.ratings, function(rating, key) {
                if(rating.type === 'CROSS_SELL' && rating.advancedRatingConfig) {
                    rating.tileClass = rating.advancedRatingConfig.cross_sell.modelingStrategy;
                } else {
                    rating.tileClass = rating.type;
                }

                if(rating.type === 'CROSS_SELL' || rating.type === 'CUSTOM_EVENT') {
                    rating.chartConfig = vm.barChartLiftConfig;
                } else {
                    rating.chartConfig = vm.barChartConfig;
                }        

                var newBucketMetadata = [];
                if(rating.bucketMetadata) {
                    angular.forEach(rating.bucketMetadata, function(rating, key) {
                        rating.lift = (Math.round( rating.lift * 10) / 10).toString();
                        newBucketMetadata.push(rating);
                    });
                } else {
                    newBucketMetadata = [{
                        "bucket_name": "B",
                        "num_leads": 10,
                        "lift": "1.3"
                    },
                    {
                        "bucket_name": "A",
                        "num_leads": 11,
                        "lift": "0.3"
                    },
                    {
                        "bucket_name": "F",
                        "num_leads": 14,
                        "lift": "0.5"
                    },
                    {
                        "bucket_name": "C",
                        "num_leads": 16,
                        "lift": "0.8"
                    },
                    {
                        "bucket_name": "D",
                        "num_leads": 18,
                        "lift": "0.9"
                    }];
                }

                rating.newBucketMetadata = newBucketMetadata;

            });
        });

        console.log(arr.slice(Math.max(arr.length - 10, 1)));

    }

    vm.checkState = function(type) {
        var map = {
            'home.segment.explorer.attributes':'attributes',
            'home.segment.explorer.enumpicker':'attributes',
            'home.segment.explorer.builder':'builder',
            'home.segment.accounts':'accounts',
            'home.segment.contacts':'contacts'
        };
        
        return map[StateHistory.lastTo().name] == type;
    }

    /**
     * if they decide they want to add sorting by account or contact counts uncomment this and add
     * { label: 'Accounts', icon: 'numeric', property: 'accountCount' },
     * { label: 'Contacts', icon: 'numeric', property: 'contactCount' }
     * to sort object above
     */
    var checkForBuckets = $interval(function() {
        if(vm.current.bucketCountMap && vm.current.ratings) {
            angular.forEach(vm.current.ratings, function(rating, key) {
                rating.accountCount = vm.current.bucketCountMap[rating.id].accountCount || 0;
                rating.contactCount = vm.current.bucketCountMap[rating.id].contactCount || 0;
            });
            $interval.cancel(checkForBuckets);
        }
    }, 1000);
    vm.init();

    vm.hasRules = function(rating) {
        var hasRules = RatingsEngineStore.hasRules(rating);
        return hasRules;
    }

    vm.isAIRating = function(rating){
        if(rating && rating.type === "CROSS_SELL"){
            return true;
        }
        return false;
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
        
        var tileState = vm.current.tileStates[rating.id];

        if(tileState.editRating !== true){
            if (rating.type === 'CROSS_SELL' || rating.type === 'CUSTOM_EVENT') {
                RatingsEngineStore.getRating(rating.id).then(function(engine){
                    RatingsEngineStore.setRating(engine);
                    RatingsEngineStore.getRatingModel(rating.id, engine.activeModel.AI.id).then(function(model){

                        var modelId = model.AI.modelSummary ? model.AI.modelSummary.Id : null,
                            modelSummary = model.AI.modelSummary,
                            modelJobId = model.AI.modelingJobId;

                        if ((modelSummary !== null) || (modelJobId !== null)) {
                            $state.go('home.ratingsengine.dashboard', { 
                                rating_id: rating.id, 
                                modelId: modelId
                            });
                        } else {
                            $state.go('home.ratingsengine.productpurchase', {rating_id: rating.id, engineType: model.AI.modelingStrategy, fromList: true});
                        }
                    });
                });                
            } else {
                $state.go('home.ratingsengine.dashboard', { 
                    rating_id: rating.id, 
                    modelId: '' 
                });
            } 
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

        console.log(rating, disable);
        
        if (disable && !vm.isAIRating(rating)) {
            return false;
        }

        var newStatus = (rating.status === 'ACTIVE' ? 'INACTIVE' : 'ACTIVE');
        var updatedRating = {
                id: rating.id,
                status: newStatus,
            }

        console.log(updatedRating);

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
