angular.module('pd.jobs', [
    'pd.jobs.import.credentials',
    'pd.jobs.import.file',
    'pd.jobs.import.ready',
    'pd.jobs.status'
])

.controller('JobsCtrl', function($scope, $rootScope, $http, JobsService) {
    $scope.jobs;
    $scope.showEmptyJobsMessage = false;

    JobsService.GetAllJobs().then(function(result) {
        $scope.jobs = result.resultObj;

        if ($scope.jobs) {
            $scope.showEmptyJobsMessage = false;
        } else {
            $scope.showEmptyJobsMessage = true;
        }
    });
});
