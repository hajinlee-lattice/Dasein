angular.module('mainApp.appCommon.directives.ngQtipDirective', [])
    .directive('ngQtip', function () {
        return {
            restrict: 'A',
            link: function(scope, element, attrs) {
                return $(element).qtip({
                    content: attrs.title,
                    style: 'qtip-bootstrap'
                });
            }
        };
    });