angular
.module('common.modules.menus.sortby', [])
.directive('menuSortBy',function() {
    return {
        restrict: 'EA',
        scope: {
            config:'=',
            store: '@'
        },
        templateUrl: 'app/AppCommon/modules/menus/sortby/SortByView.html',
        controller: function ($scope, $document, FilterService) {
            angular.extend($scope, $scope.config, {
                visible: false
            }, {
                init: function() {
                    if (!$scope.label) {
                        $scope.label = $scope.items[0].label;
                    }

                    if (!$scope.icon) {
                        $scope.icon = $scope.items[0].icon;
                    }

                    var filterStore = ($scope.store ? FilterService.getFilters($scope.store) : null);
                    if(filterStore) {
                        $scope.config.icon = filterStore.icon;
                        $scope.config.property = filterStore.property;
                        $scope.config.label = filterStore.label;
                        $scope.config.order = filterStore.order;

                        $scope.icon = filterStore.icon;
                        $scope.property = filterStore.property;
                        $scope.label = filterStore.label;
                        $scope.order = filterStore.order;
                    }
                },
                clickOrder: function() {
                    $scope.order = $scope.config.order = ($scope.order == '' ? '-' : '');

                    FilterService.setFilters($scope.store, {
                        label: $scope.label,
                        icon: $scope.icon,
                        property: $scope.property,
                        order: $scope.order
                    });
                },
                clickProperty: function(item) {
                    $scope.label = item.label; 
                    $scope.icon = item.icon; 
                    $scope.property = $scope.config.property = item.property;

                    FilterService.setFilters($scope.store, {
                        label: $scope.label,
                        icon: $scope.icon,
                        property: $scope.property,
                        order: $scope.order
                    });
                },
                toggle: function($event){
                    $scope.visible = !$scope.visible;
                    
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
                }
            });

            $scope.init();
        }
    };
});