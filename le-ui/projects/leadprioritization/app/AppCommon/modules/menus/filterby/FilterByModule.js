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
        controller: function ($scope, $filter) {
            angular.extend($scope, $scope.config, {
                visible: false
            }, {
                init: function() {
                    if (!$scope.label) {
                        $scope.label = $scope.items[0].label;
                    }
                },
                toggle: function() {
                    $scope.visible = !$scope.visible;

                    if ($scope.visible) {
                        $scope.items.forEach(function(item, i) {
                            item.filtered = $filter('filter')($scope.config.unfiltered, item.action, true);
                            item.total = item.filtered.length;
                            console.log(i, item.filtered);
                        });
                    }
                },
                click: function(item) {
                    $scope.label = item.label;
                    $scope.config.filtered = item.filtered;
                }
            });

            $scope.init();
        }
    };
});