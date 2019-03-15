angular.module('lp.playbook.wizard.newlaunch', [])
.component('newlaunch', {
    templateUrl: 'app/playbook/content/newlaunch/newlaunch.component.html',
    bindings: {
        featureflags: '<',
        userName: '<',
        trayuser: '<'
    },
    controller: function(
        $scope, $state, $stateParams, FeatureFlagService,
        ResourceUtility, BrowserStorageUtility, PlaybookWizardStore, PlaybookWizardService
    ) {
        var vm = this;

        angular.extend(vm, {
            recommendationCounts: PlaybookWizardStore.getRecommendationCounts(),
            status: $stateParams.status,
            launching: false,
            externalIntegrationEnabled: vm.featureflags.EnableExternalIntegration,
            destinationOrg: PlaybookWizardStore.crmselection_form ? PlaybookWizardStore.crmselection_form.crm_selection : {},
            audienceId: "",
            audienceName: "",
            folderName: "",
            createNewList: true,
            useExistingList: false,
            staticLists: [],
            listSelection: {},
            userAccessToken: null
        });

        vm.$onInit = function() {
            vm.externalAuthenticationId = vm.destinationOrg.externalAuthentication ? vm.destinationOrg.externalAuthentication.trayAuthenticationId : null;
            vm.loadingFolders = true;
            if (vm.externalIntegrationEnabled && vm.externalAuthenticationId && vm.trayuser) {
                PlaybookWizardService.getTrayAuthorizationToken(vm.trayuser).then(function(result) {
                    vm.userAccessToken = result;
                    PlaybookWizardService.getMarketoPrograms(vm.externalAuthenticationId, vm.userAccessToken).then(function(programResults) {
                        vm.programs = programResults.result;
                        vm.loadingFolders = false;
                    });
                })
            }
        }

        vm.nextSaveLaunch = function() {
            vm.launching = true;
            vm.status = "Launching...";

            PlaybookWizardStore.setAudienceId(vm.audienceId);
            PlaybookWizardStore.setAudienceName(vm.audienceName);
            PlaybookWizardStore.setMarketoProgramName(vm.programName);

            PlaybookWizardStore.nextSaveLaunch(null, {lastIncompleteLaunch: PlaybookWizardStore.currentPlay.launchHistory.lastIncompleteLaunch});
        }

        vm.updateListSelection = function() {
            console.log(vm.listSelection);
            if (vm.listSelection != null) {
                vm.audienceName =  vm.listSelection.name;
                vm.audienceId = vm.listSelection.id;
                vm.createNewList = false;
            } else {
                vm.audienceName =  "";
                vm.audienceId = "";
                vm.createNewList = true;

            }
        }

        vm.updateProgramName = function() {            
            vm.loadingLists = true;
            PlaybookWizardService.getMarketoStaticLists(vm.externalAuthenticationId, vm.userAccessToken, vm.programName).then(function(listResults) {
                vm.staticLists = listResults.result;
                vm.loadingLists = false;
            });
            
        }

        vm.isInvalidAudienceSelection = function() {
            if (vm.externalIntegrationEnabled && vm.destinationOrg.externalAuthentication && vm.destinationOrg.externalAuthentication.trayAuthenticationId) {
                return vm.createNewList ? (!vm.programName || !vm.audienceName) : (vm.listSelection == {});
            }
            return false;
        }

    }});