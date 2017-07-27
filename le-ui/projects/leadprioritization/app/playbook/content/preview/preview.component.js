angular.module('lp.playbook.wizard.preview', [])
.controller('PlaybookWizardPreview', function(
    $state, $stateParams, ResourceUtility, Play, TalkingPointPreviewResources
) {
    var vm = this;

    angular.extend(vm, {
        play: Play,
        stateParams: $stateParams
    });

    vm.init = function() {
    };

    vm.init();
});