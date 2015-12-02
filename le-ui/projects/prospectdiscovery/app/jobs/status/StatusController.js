angular.module('controllers.jobs.status', [

])

.directive('jobStatusRow', function() {
    return {
        restrict: 'E',
        templateUrl: 'app/jobs/status/JobStatusRow.html',
        scope: {
            job: '=',
        },
        controller: ['$scope', function ($scope) {
            $scope.showStatusLink = false;
            $scope.jobRowExpanded = false;
            $scope.statusLinkText;

            if ($scope.job.status == "Running") {
                $scope.showStatusLink = true;
                $scope.statusLinkText = "Cancel Job";
            } else if ($scope.job.status == "Completed") {
                $scope.showStatusLink = true;
                $scope.statusLinkText = "View Report";
            }
            
            $scope.toggleArrow = function() {
                $scope.jobRowExpanded = ! $scope.jobRowExpanded;
            }
        }]
    };
})