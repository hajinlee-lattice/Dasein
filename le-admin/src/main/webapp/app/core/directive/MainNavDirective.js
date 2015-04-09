var app = angular.module("app.core.directive.MainNavDirective", [
    'le.common.util.UnderscoreUtility',
    'ngSanitize',
    'ui.router'
]);

app.service('MainNavService', function(_, $state){
    var parseLastSegment = function(seg, params){
        if (seg.indexOf('tenantId') > -1) {
            return params.tenantId;
        }
    };

    var parseSegment = function(seg, params){
        if (seg === "tenants") {
            return "<a href='" + $state.href('STATE_TENANTS') + "'>Tenants</a>";
        }

        if (seg.indexOf('tenantId') > -1) {
            return "<a href='" + $state.href('STATE_TENANT_INFO', params) + "'>" + params.tenantId + "</a>";
        }
    };

    this.parseState = function (state, params) {
        var segments = state.url.split('/');
        if (segments[0] === '') { segments.splice(0, 1); }
        var links = _.map(segments, function(seg, idx){
            if (idx == segments.length - 1) {
                return parseLastSegment(seg, params);
            } else {
                return parseSegment(seg, params);
            }
        });
        return links;
    };
});

app.directive('mainNav', function(){
    return {
        restrict: 'E',
        templateUrl: 'app/core/view/MainNavView.html',
        scope: true,
        controller: ['$scope', '$rootScope', '$state', '$stateParams', 'MainNavService',
            function ($scope, $rootScope, $state, $stateParams, MainNavService) {
                $scope.name = "hehe";

                $rootScope.$on('$stateChangeSuccess', function () {
                    $scope.links = MainNavService.parseState($state.current, $stateParams);
                });
            }]
    };
});
