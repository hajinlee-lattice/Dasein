angular.module('mainApp.appCommon.widgets.AdminInfoWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility'
])
.controller('AdminInfoWidgetController', function ($scope, $rootScope, $http, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.Error = { ShowError: false };

    var data = $scope.data;
    $scope.ModelId = $scope.data.ModelId;

    function parseROC(score) {
        return score.toFixed(4).toString() + " Excellent";
    }

    $scope.ModelHealthScore = parseROC(data.ModelDetails.RocScore);
    $scope.LookupId = data.ModelDetails.LookupID;
})
.directive('adminInfoWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/adminInfoWidget/AdminInfoWidgetTemplate.html'
    };

    return directiveDefinitionObject;
})
.directive('fileDownloader', function() {
    return {
        restrict:    'E',
        template:   '<a href="" data-ng-click="downloadFile()" data-ng-hide="fetching">{{linkText}}</a><span data-ng-show="fetching">{{ResourceUtility.getString("MODEL_ADMIN_FETCHING")}}</span>',
        scope:       true,
        link:        function (scope, element, attr) {
            var anchor = element.children()[0];

            // When the download starts, disable the link
            scope.$on('download-start', function () {
                $(anchor).attr('disabled', 'disabled');
            });

            // When the download finishes, attach the data to the link. Enable the link and change its appearance.
            scope.$on('downloaded', function (event, data) {
                $(anchor).attr({
                    href:     'data:' + attr.filetype + ';base64,' + data,
                    download: attr.filename
                })
                .removeAttr('disabled');

                scope.downloadFile = function () { };
            });

            scope.$on('download-failed', function (event, data) {
                $(anchor).removeAttr('disabled');
            });
        },
        controller:  ['$scope', '$attrs', '$http', 'ResourceUtility', function ($scope, $attrs, $http, ResourceUtility) {
            $scope.fetching = false;
            $scope.ResourceUtility = ResourceUtility;
            $scope.linkText = ResourceUtility.getString("MODEL_ADMIN_FETCH");
            $scope.downloadFile = function() {
                $scope.fetching = true;
                $scope.$parent.Error.ShowError = false;
                $scope.$emit('download-start');
                $http.get($attrs.url).then(function (response) {
                    $scope.$emit('downloaded', btoa(unescape(encodeURIComponent(JSON.stringify(response.data)))));
                    $scope.fetching = false;
                    $scope.linkText = ResourceUtility.getString("MODEL_ADMIN_DOWNLOAD");
                }, function () {
                    $scope.$emit('download-failed');
                    $scope.$parent.Error.ShowError = true;
                    $scope.$parent.Error.ErrorMsg = ResourceUtility.getString("MODEL_ADMIN_FETCH_ERROR", [$attrs.filename]);
                    $scope.fetching = false;
                    $scope.linkText = ResourceUtility.getString("MODEL_ADMIN_FETCH");
                });
            };
        }]
    };
});