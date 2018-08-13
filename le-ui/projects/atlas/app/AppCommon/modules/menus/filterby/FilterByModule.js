angular
.module('common.modules.menus.filterby', [])
.filter("as", function($parse) {
  return function(value, context, path) {
    return $parse(path).assign(context, value);
  };
})
.directive('menuFilterBy',function() {
    return {
        restrict: 'EA',
        scope: {
            config:'=',
            store: '@',
            callback: '&?callbackFunction',
            current: '=?'
        },
        templateUrl: 'app/AppCommon/modules/menus/filterby/FilterByView.html',
        controller: function ($scope, $filter, $document, FilterService) {
            angular.extend($scope, $scope.config, {
                visible: false
            }, {
                init: function() {
                    if (!$scope.label) {
                        $scope.label = $scope.items[0].label;
                    }

                    var filterStore = ($scope.store ? FilterService.getFilters($scope.store) : null);
                    if(filterStore) {
                        $scope.config.label = filterStore.label;
                        $scope.config.value = filterStore.value;
                        $scope.config.items = filterStore.items;
                        $scope.config.filtered = filterStore.filtered;
                        $scope.config.unfiltered = filterStore.unfiltered;

                        $scope.label = filterStore.label;
                        $scope.value = filterStore.value;
                        $scope.items = filterStore.items;
                        $scope.filtered = filterStore.filtered;
                        $scope.unfiltered = filterStore.unfiltered;
                    }
                },
                toggle: function($event) {

                    $scope.visible = !$scope.visible;

                    if ($scope.visible) {
                        $scope.items.forEach(function(item, i) {
                            if(typeof ($scope.callback) === 'undefined') {
                                item.filtered = $filter('filter')($scope.config.unfiltered, item.action, true);
                                item.total = item.filtered.length;
                            }
                        });
                    }
                    if($event && $event.target) {
                        var target = angular.element($event.target),
                        parent = target.parent();
                        var click = function($event){
                            var clicked = angular.element($event.target),
                            inside = clicked.closest(parent).length;
                            if(!inside) {
                                $scope.visible = false;
                                $scope.$digest();
                                $document.unbind('click', click);
                            }
                        }
                        $document.bind('click', click);
                    }
                },
                click: function(item) {
                    $scope.current = 1;

                    $scope.label = item.label;
                    $scope.config.filtered = item.filtered;
                    $scope.config.value = item.action;

                    $scope.callCallback = function () {
                        if (typeof ($scope.callback) != 'undefined'){
                            $scope.callback({args:Object.values(item)});
                        }
                    }

                    $scope.callCallback();

                    FilterService.setFilters($scope.store, {
                        label: item.label,
                        value: item.action,
                        items: $scope.items,
                        filtered: item.filtered,
                        unfiltered: $scope.unfiltered,
                        callback: $scope.callback
                    });

                }
            });

            $scope.init();
        }
    };
});