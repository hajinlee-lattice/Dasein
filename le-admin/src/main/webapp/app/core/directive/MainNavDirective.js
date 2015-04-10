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
        if (seg === "tenants") {
            return "Tenants";
        }

    };

    var parseSegment = function(seg, params){
        if (seg === "tenants") {
            return "<a href='" + $state.href('TENANTS') + "'>" + parseLastSegment(seg, params) + "</a>";
        }

        if (seg.indexOf('tenantId') > -1) {
            return "<a href='" + $state.href('TENANT_INFO', params) + "'>" + parseLastSegment(seg, params) + "</a>";
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
                $scope.links = MainNavService.parseState($state.current, $stateParams);
                $rootScope.$on('$stateChangeSuccess', function () {
                    $scope.links = MainNavService.parseState($state.current, $stateParams);
                });
            }]
    };
});
