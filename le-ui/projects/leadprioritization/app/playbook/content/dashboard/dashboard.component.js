angular.module('lp.playbook.dashboard', [
    'mainApp.appCommon.utilities.TimestampIntervalUtility'
])
.controller('PlaybookDashboard', function(
    $q, $scope, $stateParams, $state, $interval, $rootScope,
    PlaybookWizardStore, TimestampIntervalUtility, NumberUtility, QueryStore
) {

    var vm = this,
        play_name = $stateParams.play_name,
        onpage = true,
        launchButtonStates = {
            initial: {
                label: 'Launch',
                state: ''
            },
            Launching: {
                label: 'Launching'
            },
            Launched: {
                label: 'Re-Launch Now'
            }
        };

    angular.extend(vm, {
        TimestampIntervalUtility: TimestampIntervalUtility,
        NumberUtility: NumberUtility,
        launchHistory: [],
        invalid: [],
        editable: true,
        play: null,
        launchButton: angular.copy(launchButtonStates.initial),
        showLaunchSpinner: false,
        editing: {},
        barChartConfig: PlaybookWizardStore.barChartConfig,
        barChartLiftConfig: PlaybookWizardStore.barChartLiftConfig
    });

    // $q.when($stateParams.play_name, function() {
    //     if(play_name) {
    //         PlaybookWizardStore.getPlayLaunches(play_name, 'Launched').then(function(results){
    //             vm.launchHistory = results;
    //         });
    //     }
    // });
    
    /**
     * contenteditable elements convert to html entities, so I removed it but want to keep this 
     * function because it could be useful if I figure out a way around this issue
     */
    vm.keydown = function($event, max, debug) {
        var element = angular.element($event.currentTarget),
            html = element.html();
            length = html.length,
            max = max || 50,
            allowedKeys = [8, 35, 36, 37, 38, 39, 40, 46]; // up, down, home, end, delete, backspace, things like that go in here

        if(debug) {
            console.log('pressed', $event.keyCode, 'length', length, 'html', html);
        }
        
        if(length > (max - 1) && allowedKeys.indexOf($event.keyCode) === -1) {
            $event.preventDefault();
        }

    }

    vm.hasLiftChart = function(play) {
        return play.ratingEngine.type === 'CROSS_SELL' || play.ratingEngine.type === 'CUSTOM_EVENT';
    }

    vm.removeSegment = function(play) {
        PlaybookWizardStore.removeSegment(play);
    }
    
    vm.launchPlay = function() {
        if(!vm.showLaunchSpinner) {
            vm.showLaunchSpinner = true;
            PlaybookWizardStore.launchPlay(vm.play).then(function(data) {
                vm.launchHistory.push(data);
                vm.showLaunchSpinner = false;
                if(onpage) {
                    $state.go('home.playbook.dashboard.launch_job', {play_name: vm.play.name, applicationId: data.applicationId});
                }
            });
        }
    }

    var findByPath = function(path, obj) {
        function index(obj,i) {
            return obj[i]
        }
        return path.split('.').reduce(index, obj);
    }

    // var makeSimpleGraph = function(buckets, path) {
    //     console.log(buckets);
    //     var total =  0;
    //     function index(obj,i) {
    //         return obj[i]
    //     }
    //     for (var i in buckets) {
    //         var bucket = buckets[i];
    //         console.log(path.split('.').reduce(index, bucket));
    //         total += (path ? path.split('.').reduce(index, bucket) : buckets[i]);
    //     }

    //     return {
    //         total: total,
    //         buckets: buckets
    //     }
    // };

    // PLS-8472 Using play.ratingEngine.bucketMetadata instead of play.ratings to generate play ratings graph
    // Delete this method when play.ratings becomes more reliable
    var makeSimpleGraph = function(buckets, path) {
        var total =  0;
        function index(obj,i) {
            return obj[i]
        }
        for (var i in buckets) {
            var bucket = buckets[i];
            total += (path ? path.split('.').reduce(index, bucket) : buckets[i]);
        }

        buckets.forEach(function(bkt) {
            if (!bkt.height) {
                bkt.height = (bkt.count / total) * 100;
            }
        });
        
        return {
            total: total,
            buckets: buckets
        }
    };

    var makeLaunchGraph = function(launchHistory) {
        if(!launchHistory || !launchHistory.playLaunch) {
            return false;
        }
        var total_contacts = launchHistory.playLaunch.contactsLaunched + launchHistory.newContactsNum,
            total_accounts = launchHistory.playLaunch.accountsLaunched + launchHistory.newAccountsNum,
            total = total_contacts + total_accounts;
            
        return {
            buckets: {
                contacts: {
                    new: launchHistory.newContactsNum,
                    current: launchHistory.playLaunch.contactsLaunched,
                    total: total_contacts
                },
                accounts: {
                    new: launchHistory.newAccountsNum,
                    current: launchHistory.playLaunch.accountsLaunched,
                    total: total_accounts
                }
            },
            total: total
        };
    }

    vm.autofocus = function($event) {
        var element = angular.element($event.currentTarget),
            target = element.find('[autofocus]');

        target.focus();
        // set focus and put cursor at begining
        setTimeout(function() {
            target.focus(); // because textareas
            target[0].setSelectionRange(0, 0);
        }, 10);
    }

    vm.edited = function(property) {
        if(!vm.editing[property]) {
            return false;
        }

        var content = vm.editing[property],
            newPlay = angular.copy(vm.play),
            save = false;

        newPlay[property] = content;

        if(vm.play[property] != newPlay[property]) {
            save = true;
            // if(property === 'displayName' && !content) {
            //     save = false;
            //     $element.text(vm.play[property]);
            // }
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
                // backconfig.backName = vm.play.displayName;
                // console.log('backconfig', backconfig);
                // $scope.$apply();

            });
        }
    }

    vm.launchValidate = function(play) {
        var properties = [
                'displayName',
                'ratingEngine',
            ];

        properties.forEach(function(property){
            if(!findByPath(property,play) && vm.invalid.indexOf(property) == -1) {
                vm.invalid.push(property);
            }
        });
        return (vm.invalid.length ? false : true);
    };

    var checkLaunchState;
    vm.getPlay = function() {
        PlaybookWizardStore.getPlay(play_name, true).then(function(play){
            vm.play = play;

            vm.launchButton = PlaybookWizardStore.launchButton(play);
            vm.launchedState = vm.launchButton.state; //(vm.play.launchHistory && vm.play.launchHistory.mostRecentLaunch && vm.play.launchHistory.mostRecentLaunch.launchState ? vm.play.launchHistory.mostRecentLaunch.launchState : null);
            
            // vm.ratingsGraph = makeSimpleGraph(play.ratings && play.ratings, 'count');

            // PLS-8472 Using play.ratingEngine.bucketMetadata instead of play.ratings to generate play ratings graph once ratings object is more stable
            buckets = reformatBucketMetadata(play.ratingEngine.bucketMetadata);
            vm.ratingsGraph = makeSimpleGraph(buckets, 'count');
   
            if(play.ratingEngine.type === 'CROSS_SELL' && play.ratingEngine.advancedRatingConfig) {
                    play.ratingEngine.tileClass = play.ratingEngine.advancedRatingConfig.cross_sell.modelingStrategy;
                } else {
                    play.ratingEngine.tileClass = play.ratingEngine.type;
                }

                if(play.ratingEngine.type === 'CROSS_SELL' || play.ratingEngine.type === 'CUSTOM_EVENT') {
                    play.ratingEngine.chartConfig = vm.barChartLiftConfig;
                } else {
                    play.ratingEngine.chartConfig = vm.barChartConfig;
                }        

                var newBucketMetadata = [];

                if(play.ratingEngine.bucketMetadata && play.ratingEngine.bucketMetadata.length > 0) {
                    angular.forEach(play.ratingEngine.bucketMetadata, function(rating, key) {
                        rating.lift = (Math.round( rating.lift * 10) / 10).toString();
                        newBucketMetadata.push(rating);
                    });
                }
                play.ratingEngine.newBucketMetadata = newBucketMetadata;

            vm.launchGraph = makeLaunchGraph(play.launchHistory);

            if(vm.launchedState === 'Launching') { // if it's in a launching state check every 10 seconds so we can update the button, then stop checking
                vm.showLaunchSpinner = true;

                checkLaunchState = $interval(function() {
                    PlaybookWizardStore.getPlayLaunches(play_name).then(function(results) {
                        var result = results[0];
                        vm.launchHistory = results;
                        vm.launchedState = (result && result.launchState ? result.launchState : null);

                        vm.launchButton = PlaybookWizardStore.launchButton(play, vm.launchedState);
                        if(vm.launchedState === 'Launched' || vm.launchedState === 'Failed') {
                            $interval.cancel(checkLaunchState);
                            vm.showLaunchSpinner = false;
                        }
                    });
                }, 10 * 1000);
            }
        });
    }

    reformatBucketMetadata = function(bucketMetadata) {
        buckets = [];
        bucketMetadata.forEach(function(bkt) {
            buckets.push({
                bucket: bkt.bucket_name,
                count: bkt.num_leads
            })
        });
        return buckets;
    }

    $scope.$on('$destroy', function() {
        onpage = false;
        $interval.cancel(checkLaunchState);
        checkLaunchState = null;
    });

    PlaybookWizardStore.clear();
    if(play_name) {
        vm.getPlay();
    }

});
