angular
    .module('pd.navigation.pagination', [])
    /* 
        startFrom filter combined with limitTo makes pagination possible 
    */
    .filter('startFrom', function () {
        return function (input, start) {
            if (input) {
                start = +start;
                return input.slice(start);
            }
            return [];
        };
    })
    .directive('pdPaginationControls', function() {
        return {
            restrict: 'EA',
            templateUrl: 'app/navigation/pagination/PaginationView.html',
            scope: {
                max: '=',
                current: '='
            },
            controller: ['$scope', function ($scope) { 
                $scope.Math = window.Math;
            }]
        };
    }
);