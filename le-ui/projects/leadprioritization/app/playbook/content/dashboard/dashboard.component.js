angular.module('lp.playbook.dashboard', [
    'mainApp.appCommon.utilities.TimestampIntervalUtility'
])
.controller('PlaybookDashboard', function(
    $q, $stateParams, $state, $interval,
    PlaybookWizardStore, TimestampIntervalUtility, NumberUtility, QueryStore
) {
    var vm = this,
        play_name = $stateParams.play_name;

    angular.extend(vm, {
        TimestampIntervalUtility: TimestampIntervalUtility,
        NumberUtility: NumberUtility,
        launchHistory: [],
        invalid: [],
        editable: true,
        play: null
    });

    // $q.when($stateParams.play_name, function() {
    //     if(play_name) {
    //         PlaybookWizardStore.getPlayLaunches(play_name, 'Launched').then(function(results){
    //             vm.launchHistory = results;
    //         });
    //     }
    // });

    vm.removeSegment = function(play) {
        PlaybookWizardStore.removeSegment(play);
    }

    vm.launchPlay = function() {
        vm.showLaunchSpinner = true;
        PlaybookWizardStore.launchPlay(vm.play).then(function(data) {
            vm.launchHistory.push(data);
            vm.showLaunchSpinner = false;
            $state.go('home.playbook.dashboard.launch_job', {play_name: vm.play.name, applicationId: data.applicationId});
        });
    }

    var findByPath = function(path, obj) {
        function index(obj,i) {
            return obj[i]
        }
        return path.split('.').reduce(index, obj);
    }

    var makeSimpleGraph = function(buckets, path) {
        var total =  0;
        function index(obj,i) {
            return obj[i]
        }
        for (var i in buckets) {
            var bucket = buckets[i];
            total += (path ? path.split('.').reduce(index, bucket) : buckets[i]);
        }

        return {
            total: total,
            buckets: buckets
        }
    };

    var makeLaunchGraph = function(launchHistory) {
        if(!launchHistory || !launchHistory.playLaunch) {
            return false;
        }
        var total_contacts = launchHistory.playLaunch.contactsNum + launchHistory.newContactsNum,
            total_accounts = launchHistory.playLaunch.accountsNum + launchHistory.newAccountsNum,
            total = total_contacts + total_accounts;

        return {
            buckets: {
                contacts: {
                    new: launchHistory.newContactsNum,
                    current: launchHistory.playLaunch.contactsNum,
                    total: total_contacts
                },
                accounts: {
                    new: launchHistory.newAccountsNum,
                    current: launchHistory.playLaunch.accountsNum,
                    total: total_accounts
                }
            },
            total: total
        };
    }

    vm.edited = function($event, property) {
        var $element = angular.element($event.target),
            content = $element.text().trim(),
            newPlay = angular.copy(vm.play),
            save = false;

        newPlay[property] = content;

        if(vm.play[property] != newPlay[property]) {
            save = true;
            if(property === 'displayName' && !content) {
                save = false;
                $element.text(vm.play[property]);
            }
        }

        if(save) {
            vm.editable = false; // block rapid edits
            savePlay = {
                name: vm.play.name,
                createdBy: vm.play.createdBy,
                displayName: newPlay.displayName,
                description: newPlay.description
            }
            PlaybookWizardStore.savePlay(savePlay).then(function(play){
                vm.play = play;
                vm.editable = true;
            });
        }
    }

    vm.launchValidate = function(play) {
        var properties = [
                'displayName',
                'segment',
                'rating',
            ];

        properties.forEach(function(property){
            if(!findByPath(property,play) && vm.invalid.indexOf(property) == -1) {
                vm.invalid.push(property);
            }
        });
        return (vm.invalid.length ? false : true);
    };

    var getPlay = function() {
        PlaybookWizardStore.getPlay(play_name).then(function(play){
            vm.play = play;
            vm.launchedState = (vm.play.launchHistory && vm.play.launchHistory.mostRecentLaunch && vm.play.launchHistory.mostRecentLaunch.launchState ? vm.play.launchHistory.mostRecentLaunch.launchState : null);
            vm.ratingsGraph = makeSimpleGraph(vm.play.rating && vm.play.rating.bucketInformation, 'bucketCount');
            vm.launchGraph = makeLaunchGraph(vm.play.launchHistory);
            vm.launchValidate(play);

            if(vm.launchedState === 'Launching') { // if it's in a launching state check every 10 seconds so we can update the button, then stop checking
                vm.showLaunchSpinner = true;
                var checkLaunchState = $interval(function(){
                    PlaybookWizardStore.getPlayLaunches(play_name).then(function(results){
                        var result = results[0];
                        vm.launchHistory = results;
                        vm.launchedState = (result && result.launchState ? result.launchState : null);
                        if(vm.launchedState === 'Launched') {
                            $interval.cancel(checkLaunchState);
                            vm.showLaunchSpinner = false;
                        }
                    });
                }, 10000);
            }
        });
    }


    //PlaybookWizardStore.clear();
    if(play_name) {
        getPlay();
    }

});
