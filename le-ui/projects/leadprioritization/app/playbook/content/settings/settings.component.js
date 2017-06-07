angular.module('lp.playbook.wizard.settings', [])
.controller('PlaybookWizardSettings', function(
    $state, $stateParams, $timeout, ResourceUtility, PlaybookWizardStore
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.settings_form
    });

    vm.init = function() {
        if($stateParams.play_name) {
            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(data){
                // test play_name: play__cf21f5b9-c513-4076-bac5-dcb33fb076a7
                console.log('get play', data);
                vm.stored.play_name = data.name;
                vm.stored.play_display_name = data.display_name;
                vm.stored.description = data.description;
                if(vm.stored.play_name) {
                    $state.transitionTo('home.playbook.wizard.settings', {play_name: vm.stored.play_name}, {notify: false});
                }
           });
        }
        //$stateParams.playId = vm.stored.playId;
    };

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        PlaybookWizardStore.setValidation('settings', form.$valid);
        var opts = {};
        if(vm.stored.play_display_name) { //PlaybookWizardStore.getValidation('settings')) {
            opts.display_name = vm.stored.play_display_name;
            opts.description = vm.stored.description;
           //  PlaybookWizardStore.savePlay(opts).then(function(data){
           //      console.log('saved play', data);
           //      vm.stored.play_name = data.play_name;
           //      $state.transitionTo('home.playbook.wizard.settings', {play_name: vm.stored.play_name}, {notify: false});
           // });
        }
    }

    vm.init();
});