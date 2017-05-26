angular.module('lp.playbook.wizard.segment', [])
.controller('PlaybookWizardSegment', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore
) {
    var vm = this;

    angular.extend(vm, {
        parent_name: $state.current.name.split('.')[3],
        full_name: $state.current.name,
        customFieldsIgnore: []
    });

    vm.init = function() {
        vm.customFields = ImportWizardStore.getCustomFields(vm.parent_name);

        vm.customFields.forEach(function(item){
            vm.customFieldsIgnore[item.CustomField] = false;
        });

        vm.toggleIgnores = function($event) {
            var target = $event.target;
            vm.customFields.forEach(function(item){
                vm.customFieldsIgnore[item.CustomField] = target.checked;
            });
        }

    }

    vm.init();
});