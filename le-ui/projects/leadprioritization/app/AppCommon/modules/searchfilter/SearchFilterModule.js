angular
.module('common.modules.searchfilter', [])
.directive('searchFilter',function() {
    return {
        restrict: 'EA',
        scope: {
            query:'='
        },
        templateUrl: 'app/AppCommon/modules/searchfilter/SearchFilterView.html',
        controller: function ($scope) {
            angular.extend($scope, $scope.config, {
                visible: false
            }, {
                init: function() {
                    console.log('searchfilter', $scope);
                },
                clickToggle: function() {
                    $scope.visible = !$scope.visible; 
                    $scope.query = '';
                }
            });

            $scope.init();
        }
    };
});