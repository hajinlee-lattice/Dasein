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
        ResourceUtility, BrowserStorageUtility, PlaybookWizardStore, PlaybookWizardService, Banner
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
            audienceType: "",
            folderName: "",
            createNewList: true,
            useExistingList: false,
            staticLists: [],
            listSelection: {},
            userAccessToken: null,
            mostRecentLaunch: getMostRecentLaunch()
        });

        vm.$onInit = function() {
            vm.externalAuthenticationId = vm.destinationOrg.externalAuthentication ? vm.destinationOrg.externalAuthentication.trayAuthenticationId : null;
            vm.loadingFolders = true;
            if (vm.externalIntegrationEnabled && vm.externalAuthenticationId && vm.trayuser) {
                PlaybookWizardService.getTrayAuthorizationToken(vm.trayuser).then(function(result) {
                    vm.userAccessToken = result;
                    switch (vm.destinationOrg.externalSystemName) {
                        case "Marketo":
                            vm.getMarketoPrograms();
                            break;
                        case "Facebook":
                        case "LinkedIn":
                        default:
                            break;
                    }
                })
            }
        }

        vm.nextSaveLaunch = function() {
            vm.launching = true;
            vm.status = "Launching...";

            PlaybookWizardStore.setAudienceId(vm.audienceId);
            PlaybookWizardStore.setAudienceName(vm.audienceName);
            PlaybookWizardStore.setMarketoProgramName(vm.programName);
            PlaybookWizardStore.setChannelConfig(constructChannelConfig());

            PlaybookWizardStore.nextSaveLaunch(null, {lastIncompleteLaunch: PlaybookWizardStore.currentPlay.launchHistory.lastIncompleteLaunch});
        }

        vm.getMarketoPrograms = function() {
            var mostRecentProgramName = vm.mostRecentLaunch != null ? vm.mostRecentLaunch.folderName : '';
            PlaybookWizardService.getMarketoPrograms(vm.externalAuthenticationId, vm.userAccessToken).then(function(programResults) {
                vm.programs = programResults.result;
                vm.loadingFolders = false;
                if (vm.programs != undefined) {
                    var mostRecentProgram = vm.programs.filter((program) => {
                        return program.name == mostRecentProgramName;
                    });
                    if (mostRecentProgram.length == 1 && mostRecentProgram[0].name) {
                        vm.programName = mostRecentProgram[0].name;
                        vm.updateProgramName(true);
                    }
                } else {
                    Banner.error({message: "Error retrieving Marketo programs. Please retry later."});
                }
            });
        }

        vm.updateListSelection = function(onInit) {
            console.log(vm.listSelection);
            if (vm.listSelection != null) {
                vm.audienceName =  vm.listSelection.name;
                vm.audienceId = vm.listSelection.id;
                vm.createNewList = false;
            } else {
                vm.audienceName =  onInit ? (vm.mostRecentLaunch.audienceName || "") : "";
                vm.audienceId = "";
                vm.createNewList = true;
            }
            vm.updatePlayStore();
        }

        vm.updateProgramName = function(onInit) {           
            vm.loadingLists = true;
            PlaybookWizardService.getMarketoStaticLists(vm.externalAuthenticationId, vm.userAccessToken, vm.programName).then(function(listResults) {
                vm.staticLists = listResults.result;
                vm.loadingLists = false;
                if (onInit) {
                    var mostRecentAudience = vm.staticLists.filter((list) => {
                        return list.name == vm.mostRecentLaunch.audienceName;
                    });
                    if (mostRecentAudience.length == 1 && mostRecentAudience[0].name) {
                        vm.listSelection = mostRecentAudience[0];
                    } else {
                        vm.listSelection = null;
                    }
                    vm.updateListSelection(onInit);
                }
            });
        }

        vm.updatePlayStore = function() {
            PlaybookWizardStore.setAudienceId(vm.audienceId);
            PlaybookWizardStore.setAudienceName(vm.audienceName);
            PlaybookWizardStore.setMarketoProgramName(vm.programName);
            PlaybookWizardStore.setChannelConfig(constructChannelConfig());
        }

        vm.isValidAudienceName = function() {
            if (vm.createNewList) {
                return !vm.staticLists.some(function(list) {
                    return list.name == vm.audienceName;
                });
            }
        }

        vm.isInvalidAudienceSelection = function() {
            if (vm.externalIntegrationEnabled && vm.destinationOrg.externalAuthentication && vm.destinationOrg.externalAuthentication.trayAuthenticationId) {
                if (vm.destinationOrg.externalSystemName == "Marketo") {
                    return vm.createNewList ? (!vm.programName || !vm.audienceName || !vm.isValidAudienceName()) : (vm.listSelection == {});
                } else if (vm.destinationOrg.externalSystemName == "LinkedIn" || vm.destinationOrg.externalSystemName == "Facebook") {
                    return !vm.audienceName || !vm.audienceType;
                }
            }
            return false;
        }

        function getMostRecentLaunch() {
            return PlaybookWizardStore.getCurrentPlay() && PlaybookWizardStore.getCurrentPlay().launchHistory 
                    ? PlaybookWizardStore.getCurrentPlay().launchHistory.mostRecentLaunch 
                    : null;
        }

        function constructChannelConfig() {
            var channelConfig;
            switch (vm.destinationOrg.externalSystemName) {
                case "LinkedIn":
                    channelConfig = {
                        linkedin: {
                            audienceName: vm.audienceName,
                            audienceType: vm.audienceType
                        }
                    }
                    break;
                case "Facebook":
                    channelConfig = {
                        facebook: {
                            audienceName: vm.audienceName,
                            audienceType: vm.audienceType
                        }
                    }
                    break;
                default:
                    channelConfig = {};
            }
            return channelConfig;
        }

    }});