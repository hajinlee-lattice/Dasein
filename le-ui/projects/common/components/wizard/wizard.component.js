angular.module('common.wizard', [
    'common.wizard.progress',
    'common.wizard.controls'
])
.controller('ImportWizard', function(
    $state, $stateParams, $scope, FeatureFlagService, ResourceUtility
) {
    var vm = this,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        title: ''
    });

    vm.init = function() {
        var name = $state.current.name.split('.')[3];
        vm.title = name;
    }

    vm.init();
});