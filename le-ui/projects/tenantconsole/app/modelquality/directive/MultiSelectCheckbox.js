angular.module('app.modelquality.directive.MultiSelectCheckbox', [
])
.directive('multiSelectCheckbox', function ($timeout) {
    return {
        restrict: 'AE',
        scope: {
            options: '=',
            selected: '=',
            key: '@'
        },
        templateUrl: 'app/modelquality/view/MultiSelectCheckboxView.html',
        controller: 'MultiSelectCheckboxCtrl',
        controllerAs: 'vm'
    };
})
.controller('MultiSelectCheckboxCtrl', function ($scope) {

});
