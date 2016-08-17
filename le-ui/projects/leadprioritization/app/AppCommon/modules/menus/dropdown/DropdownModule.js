angular
.module('common.modules.menus.dropdown', [])
.directive('menuDropdown',function() {
    return {
        restrict: 'EA',
        scope: {
            config:'='
        },
        templateUrl: 'app/AppCommon/modules/menus/dropdown/DropdownView.html',
        controller: function ($scope) {
            angular.extend($scope, $scope.config, {
                visible: false
            }, {
                init: function() {
                    console.log('dropdown', $scope);
                }
            });

            $scope.init();
        }
    };
});