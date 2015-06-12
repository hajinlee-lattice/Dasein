(function(){
    angular.module('le.common.directives.ngQtipDirective', [])
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
}).call();