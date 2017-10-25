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
            config:'='
        },
        templateUrl: 'app/AppCommon/modules/menus/filterby/FilterByView.html',
        controller: function ($scope, $filter, $document) {
            angular.extend($scope, $scope.config, {
                visible: false
            }, {
                init: function() {
                    if (!$scope.label) {
                        $scope.label = $scope.items[0].label;
                    }
                },
                toggle: function($event) {
                    $scope.visible = !$scope.visible;

                    if ($scope.visible) {
                        $scope.items.forEach(function(item, i) {
                            item.filtered = $filter('filter')($scope.config.unfiltered, item.action, true);
                            item.total = item.filtered.length;
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
                    $scope.label = item.label;
                    $scope.config.filtered = item.filtered;
                    $scope.config.value = item.action;
                }
            });

            $scope.init();
        }
    };
});