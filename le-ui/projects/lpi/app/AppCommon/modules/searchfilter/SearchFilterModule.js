angular
.module('common.modules.searchfilter', [])
.directive('searchFilter',function() {
    return {
        restrict: 'EA',
        scope: {
            query:'=',
            current: '='
        },
        templateUrl: 'app/AppCommon/modules/searchfilter/SearchFilterView.html',
        controller: function ($scope) {
            angular.extend($scope, $scope.config, {
                visible: false
            }, {
                init: function() {
                },
                clickToggle: function() {
                    $scope.visible = !$scope.visible;

                    if ($scope.visible) {
                        // timeout needed, or else it only works once in chrome
                        setTimeout(function() {
                            $('div.select-menu input.form-control').focus();
                        }, 250);
                    } else {
                        $scope.current = 1;
                    }

                    $scope.query = '';
                },
                resetCurrentPage: function() {
                    if ($scope.query) {
                        $scope.current = 1;
                    }

                }
            });

            $scope.init();
        }
    };
});