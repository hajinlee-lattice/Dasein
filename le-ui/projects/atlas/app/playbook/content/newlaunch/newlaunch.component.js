angular.module('lp.playbook.wizard.newlaunch', [])
.component('newlaunch', {
    templateUrl: 'app/playbook/content/newlaunch/newlaunch.component.html',
    controller: function(
        $scope, $state, $stateParams,
        ResourceUtility, BrowserStorageUtility, PlaybookWizardStore, PlaybookWizardService
    ) {
        var vm = this;

        angular.extend(vm, {
            recommendationCounts: PlaybookWizardStore.getRecommendationCounts()
        });

        angular.extend(vm, {
            status: $stateParams.status
        });

        vm.$onInit = function() {
        }

        vm.nextSaveLaunch = function() {
            PlaybookWizardStore.nextSaveLaunch();
        }

    }});