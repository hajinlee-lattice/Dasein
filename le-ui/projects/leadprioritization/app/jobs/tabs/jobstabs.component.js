angular.module('lp.jobs')
.filter('jobfilter', function() {
    return function(jobs, type) {
        // jobType: 'processAnalyzeWorkflow';
        var retJobs = [];
        var count = 0;
        jobs.forEach(function(job) {
            if(job.jobType === type){
                retJobs.push(job);
            }
        });
        console.log('JOBs', retJobs);
        return retJobs;
    };
  })
.controller('JobsTabsController', function (
    $state, $stateParams, $scope, FeatureFlagService, JobsStore
) {
    var vm = this,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        stateParams: $stateParams,
        cdlEnabled: FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL),
        JobsStore: JobsStore
    });
});
