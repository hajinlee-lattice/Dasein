angular.module('lp.playbook.wizard.preview', [])
.controller('PlaybookWizardPreview', function(
    $state, $stateParams, ResourceUtility, Play, TalkingPointPreviewResources, CgTalkingPointStore
) {
    var vm = this;

    angular.extend(vm, {
        play: Play,
        stateParams: $stateParams,
        published: null,
        showPublishingSpinner: false
    });

    vm.init = function() {
    };

    vm.publish = function() {
        vm.showPublishingSpinner = true;
        CgTalkingPointStore.publishTalkingPoints(vm.play.name).then(function(results){
            vm.published = results;
            vm.showPublishingSpinner = false;
            $state.go('home.playbook.dashboard', {play_name: vm.play.name});
        });
    }

    vm.init();
});