angular.module('lp.jobs')
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
