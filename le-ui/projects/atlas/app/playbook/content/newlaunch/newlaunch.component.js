angular.module('lp.playbook.wizard.newlaunch', [])
.component('newlaunch', {
    templateUrl: 'app/playbook/content/newlaunch/newlaunch.component.html',
    controller: function(
        $scope, $state, $stateParams,
        ResourceUtility, BrowserStorageUtility, PlaybookWizardStore, PlaybookWizardService
    ) {
        var vm = this;

        angular.extend(vm, {
            recommendationCounts: PlaybookWizardStore.getRecommendationCounts(),
            status: $stateParams.status,
            launching: false
        });

        vm.$onInit = function() {
        }

        vm.nextSaveLaunch = function() {
            vm.launching = true;
            vm.status = "Launching...";
            PlaybookWizardStore.nextSaveLaunch(null, {lastIncompleteLaunch: PlaybookWizardStore.currentPlay.launchHistory.lastIncompleteLaunch});
        }

    }});