angular.module('lp.playbook.dashboard.launch_history', [])
.controller('PlaybookDashboardLaunchHistory', function(
    $state, $stateParams, $timeout, ResourceUtility, PlaybookWizardStore, LaunchHistoryData
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.settings_form,
        launches: LaunchHistoryData,
        cumulativeStats: LaunchHistoryData.cumulativeStats,
        summaryData: {},
        launching: false
    });

    vm.init = function() {

        console.log($state);

        if($state.current.name === 'home.playbook.plays.launch_history'){
            vm.allPlaysHistory = true;
        } else {
            vm.allPlaysHistory = false;
        }

        // if($stateParams.play_name) {
        //     PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){

        //         // console.log(play);

        //         vm.stored.play_name = play.name;
        //         vm.stored.play_display_name = play.displayName;
        //         vm.stored.play_description = play.description;
        //         if(vm.stored.play_name) {
        //             PlaybookWizardStore.setValidation('settings', true);
        //         }


        //         for (var i = 0; i < vm.launches.length; i++) {
        //             vm.totalRecoGen = vm.totalRecoGen + vm.launches[i].accountsNum;
        //             vm.totalSuppressed = 0;
        //         }


        //    });
        // }

        vm.summaryData = {
            selectedTargets: vm.cumulativeStats.selectedTargets,
            suppressed: vm.cumulativeStats.suppressed,
            errors: vm.cumulativeStats.errors,
            recommendationsLaunched: vm.cumulativeStats.recommendationsLaunched,
            contactsWithinRecommendations: vm.cumulativeStats.contactsWithinRecommendations
        }

        console.log(vm.launches);

    };

    vm.playSelectChange = function(play){
        console.log(play);
    };

    vm.play_name_required = function(){
        return !vm.stored.play_display_name;
    };

    vm.relaunchPlay = function() {

        vm.launching = true;

        PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
            PlaybookWizardStore.launchPlay(play).then(function(result){
                $state.reload();
            });
        });
    };

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        PlaybookWizardStore.setValidation('settings', form.$valid);
        if(vm.stored.play_display_name) {
            PlaybookWizardStore.setSettings({
                displayName: vm.stored.play_display_name,
                description: vm.stored.play_description
            });
        }
    }

    vm.init();
})
.filter('unique', function () {

    return function (items, filterOn) {

        if (filterOn === false) {
            return items;
        }

        if ((filterOn || angular.isUndefined(filterOn)) && angular.isArray(items)) {
            var hashCheck = {}, newItems = [];

            var extractValueToCompare = function (item) {
                if (angular.isObject(item) && angular.isString(filterOn)) {
                    return item[filterOn];
                } else {
                    return item;
                }
            };

            angular.forEach(items, function (item) {
                var valueToCheck, isDuplicate = false;

                for (var i = 0; i < newItems.length; i++) {
                    if (angular.equals(extractValueToCompare(newItems[i]), extractValueToCompare(item))) {
                        isDuplicate = true;
                        break;
                    }
                }
                if (!isDuplicate) {
                    newItems.push(item);
                }

            });
            items = newItems;
        }
        return items;
    };
});