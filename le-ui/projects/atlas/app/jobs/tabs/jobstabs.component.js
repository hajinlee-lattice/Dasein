angular.module('lp.jobs')
.filter('jobfilter', function() {
    return function(jobs, type) {
        var retJobs = [];
        var count = 0;
        jobs.forEach(function(job) {
            if(job.jobType === type){
                retJobs.push(job);
            }
        });
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

    vm.inJob = function() {
        if(vm.stateParams.inJobs === false) {
            return false;
        }
        return $state.params.jobId != undefined;
    }
});
