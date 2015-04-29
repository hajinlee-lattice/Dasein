(function(){

    var app = angular.module('app.core.directive.FileDownloaderDirective', []);

    app.directive('fileDownloader', function() {
        return {
            restrict:    'E',
            template:   '<a class="file-downloader" href="" data-ng-click="downloadFile($event)" data-ng-hide="fetching">download</a>' +
            '<span class="file-downloader" data-ng-show="fetching && !error">fetching ...</span>' +
            '<span class="file-downloader text-danger" data-ng-show="error">Downloading file error.</span>',
            scope:       {url: '@', filename: '@', filetype: '@', error: '='},
            link:        function (scope, element) {
                var anchor = element.children()[0];

                // When the download starts, disable the link
                scope.$on('download-start', function () {
                    $(anchor).attr('disabled', 'disabled');
                });

                // When the download finishes, attach the data to the link. Enable the link and change its appearance.
                scope.$on('downloaded', function (event, data) {
                    scope.downloadFile = function ($event) { };
                    $(anchor).attr({
                        href: 'data:' + scope.filetype + ';base64,' + data,
                        download: scope.filename
                    }).removeAttr('disabled');
                });

                scope.$on('download-failed', function () {
                    $(anchor).removeAttr('disabled');
                });
            },
            controller:  ['$scope', '$http', '$timeout', function ($scope, $http, $timeout) {
                $scope.fetching = false;
                $scope.fetched = false;
                $scope.firstFetch = true;

                $scope.downloadFile = function($event) {
                    $scope.fetching = true;
                    $scope.error = false;
                    $scope.$emit('download-start');

                    $http.get($scope.url).then(function (response) {
                        if ($scope.filetype === "application/json") {
                            $scope.$emit('downloaded', btoa(decodeURI(encodeURIComponent(JSON.stringify(response.data)))));
                        } else {
                            $scope.$emit('downloaded', btoa(decodeURI(encodeURIComponent(response.data))));
                        }
                        $scope.fetching = false;
                        if ($scope.firstFetch) {
                            $scope.firstFetch = false;
                            setTimeout(function(){ $event.target.click(); }, 500);
                        }
                    }, function () {
                        $scope.error = true;
                        $scope.fetching = false;
                        $scope.$emit('download-failed');
                    });
                };
            }]
        };
    });

}).call();