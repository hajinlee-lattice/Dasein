angular.module('mainApp.appCommon.directives.helperMarkDirective', [
    'mainApp.appCommon.directives.ngQtipDirective'
])
.directive('helperMark', function () {
    return {
        restrict: 'E',
        scope: {help: '@'},
        template: '<span class="has-tooltip" ng-qtip title="{{help}}"><i class="fa fa-question-circle"></i></span>'
    };
});
