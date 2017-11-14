angular.module('mainApp.appCommon.directives.input.selection', [])
    .directive('optionSpinner', function () {
        return {
            restrict: 'E',
            templateUrl: '/components/ai/input-selection.component.html',
            scope: { label: '@', hideInput: '@', hideOptions: '@' ,options: '='},
            link: function (scope, element, attrs, ctrl) {
                scope.title = '';
                scope.showTitle = true;
                scope.showInput = true;
                scope.showOptions = true;
                if (scope.label && scope.label !== '') {
                    scope.title = scope.label;
                } else {
                    scope.showTitle = false;
                }
                if (!scope.options) {
                    scope.options = [];
                }
                if (scope.hideInput === true) {
                    scope.showInput = false;
                }
                if(scope.hideOptions === true){
                    scope.showOptions = false;
                }
                if(!scope.options){
                    scope.options = [];
                }
            }
        }
    });