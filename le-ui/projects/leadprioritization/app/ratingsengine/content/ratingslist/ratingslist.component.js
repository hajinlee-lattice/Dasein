angular.module('lp.ratingsengine.ratingslist', [
    'mainApp.ratingsengine.deleteratingmodal',
    'mainApp.appCommon.directives.barchart',
    'mainApp.core.utilities.NavUtility'
])
.controller('RatingsEngineListController', function (
    $scope, $timeout, $location, $element, $state, $stateParams, $filter, $interval, $rootScope,
    RatingsEngineStore, RatingsEngineService, DeleteRatingModal, NavUtility, StateHistory
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
        }
    });

    vm.count = function(type) {
        return $filter('filter')(vm.current.ratings, { status: type }, true).length;
    }

    vm.init = function($q, $filter) {

        var arr = vm.current.ratings;
        console.log(arr.slice(Math.max(arr.length - 5, 1)));

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
                if (rating.type === 'CROSS_SELL') {
                    rating.tileClass = rating.advancedRatingConfig.cross_sell.modelingStrategy;
                } else {
                    rating.tileClass = rating.type;
                }
            });
        });

    }

    function getBarChartConfig() {
        if ($scope.barChartConfig === undefined) {

            $scope.barChartConfig = {
                'data': {
                    'tosort': true,
                    'sortBy': '-Cnt',
                    'trim': true,
                    'top': 5,
                },
                'chart': {
                    'header':'Attributes Value',
                    'emptymsg': '',
                    'usecolor': false,
                    'color': '#2E6099',
                    'mousehover': true,
                    'type': 'integer',
                    'showstatcount': true,
                    'maxVLines': 3,
                    'showVLines': false,
                },
                'vlines': {
                    'suffix': ''
                },
                'columns': [{
                    'field': 'Cnt',
                    'label': 'Records',
                    'type': 'number',
                    'chart': true,
                }]
            };
        }
        return $scope.barChartConfig;
    }

    function getBarChartLiftConfig() {
        if ($scope.barChartLiftConfig === undefined) {
            $scope.barChartLiftConfig = {
                'data': {
                    'tosort': true,
                    'sortBy': 'Lbl',
                    'trim': true,
                    'top': 5,
                },
                'chart': {
                    'header':'Value',
                    'emptymsg': '',
                    'usecolor': false,
                    'color': '#2E6099',
                    'mousehover': true,
                    'type': 'decimal',
                    'showstatcount': false,
                    'maxVLines': 3,
                    'showVLines': true,
                },
                'vlines': {
                    'suffix': 'x'
                },
                'columns': [{
                        'field': 'Lift',
                        'label': 'Lift',
                        'type': 'string',
                        'suffix': 'x',
                        'chart': true
                    }
                ]
            };
        }
        return $scope.barChartLiftConfig;
    }

    vm.getChartConfig = function (ratingType) {        
        return getBarChartLiftConfig();

        // if (ratingType === 'CROSS_SELL' || ratingType === 'CUSTOM_EVENT') {
        //     return getBarChartLiftConfig();
        // } else {
        //     return getBarChartConfig();    
        // }        
    }

    vm.getData = function () {
        var data = [{
                "Lbl": "B",
                "Cnt": 10,
                "Lift": "1.3",
                "Id": 2,
                "Cmp": "EQUAL",
                "Vals": [
                    "B"
                ]
            },
            {
                "Lbl": "A",
                "Cnt": 11,
                "Lift": "0.3",
                "Id": 1,
                "Cmp": "EQUAL",
                "Vals": [
                    "A"
                ]
            },
            {
                "Lbl": "F",
                "Cnt": 14,
                "Lift": "0.5",
                "Id": 3,
                "Cmp": "EQUAL",
                "Vals": [
                    "F"
                ]
            },
            {
                "Lbl": "C",
                "Cnt": 16,
                "Lift": "0.8",
                "Id": 3,
                "Cmp": "EQUAL",
                "Vals": [
                    "C"
                ]
            },
            {
                "Lbl": "D",
                "Cnt": 18,
                "Lift": "0.9",
                "Id": 3,
                "Cmp": "EQUAL",
                "Vals": [
                    "D"
                ]
            }
        ];
        // console.log('Data ',data);
        return data;
    }
    vm.getAttributeRules = function() {

        return 124;

        // var attributes = QueryStore.getDataCloudAttributes(true);
        
        // attributes = attributes.filter(function(item) {
            
        //     var restriction = item.bucketRestriction,
        //         isSameAttribute = restriction.attr == attribute.Entity + '.' + (attribute.Attribute || attribute.ColumnId),
        //         isSameBucket = true,
        //         bkt = restriction.bkt;
        //     var ret = QueryTreeService.getAttributeRules(restriction, bkt, bucket, isSameAttribute);
        //     return ret;
        // });
        // return attributes;
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
            if (rating.type === 'CROSS_SELL') {
                RatingsEngineStore.getRating(rating.id).then(function(engine){
                    RatingsEngineStore.setRating(engine);
                    RatingsEngineStore.getRatingModel(rating.id, engine.activeModel.AI.id).then(function(model){
                        var modelId = model.AI.modelSummary ? model.AI.modelSummary.Id : null,
                            modelingJobId = model.AI.modelingJobId;

                        if (modelingJobId !== null) {
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
                $state.go('home.ratingsengine.dashboard', { rating_id: rating.id, modelId: '' });
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
        
        if (disable && !vm.isAIRating(rating)) {
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
