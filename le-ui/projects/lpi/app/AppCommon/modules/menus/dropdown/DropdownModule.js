angular
.module('common.modules.menus.dropdown', [])
.directive('menuDropdown',function() {
    return {
        restrict: 'EA',
        scope: {
            config:'='
        },
        templateUrl: 'app/AppCommon/modules/menus/dropdown/DropdownView.html',
        controller: function ($scope, $document) {
            angular.extend($scope, $scope.config, {
                visible: false
            }, {
                init: function() {

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