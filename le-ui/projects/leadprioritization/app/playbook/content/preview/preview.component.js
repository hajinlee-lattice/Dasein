angular.module('lp.playbook.wizard.preview', [])
.controller('PlaybookWizardPreview', function(
    $state, $stateParams, ResourceUtility, Play
) {
    var vm = this;

    angular.extend(vm, {
        play: Play
    });

    vm.init = function() {

    };

    vm.init();
});