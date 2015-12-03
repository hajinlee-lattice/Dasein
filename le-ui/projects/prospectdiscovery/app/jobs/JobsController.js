angular.module('controllers.jobs', [
    'controllers.jobs.import.credentials',
    'controllers.jobs.import.file',
    'controllers.jobs.import.ready',
    'controllers.jobs.status',
    'services.jobs'
])

.controller('JobsCtrl', function($scope, $rootScope, $http, JobsService) {
    $scope.jobs;

    JobsService.GetAllJobs().then(function(result) {
        $scope.jobs = result.resultObj;
    });
});
