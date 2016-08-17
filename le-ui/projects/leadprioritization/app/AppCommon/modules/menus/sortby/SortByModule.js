angular
.module('common.modules.menus.sortby', [])
.directive('menuSortBy',function() {
    return {
        restrict: 'EA',
        scope: {
            config:'='
        },
        templateUrl: 'app/AppCommon/modules/menus/sortby/SortByView.html',
        controller: function ($scope) {
            console.log('sortby', $scope);

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
                },
                clickOrder: function() {
                    $scope.order = $scope.config.order = ($scope.order == '' ? '-' : '');
                },
                clickProperty: function(item) {
                    console.log('click', item);
                    $scope.label = item.label; 
                    $scope.icon = item.icon; 
                    $scope.property = $scope.config.property = item.property;
                }
            });

            $scope.init();
        }
    };
});