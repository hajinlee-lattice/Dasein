angular.module('lp.playbook')
.service('PlaybookWizardStore', function($q, $state, $stateParams,  $interval,
    PlaybookWizardService, CgTalkingPointStore, BrowserStorageUtility, RatingsEngineStore, QueryStore){

    var PlaybookWizardStore = this;

    this.current = {
        plays: [],
        tileStates: {}

    };
    this.checkLaunchState = {};
    this.savedRating = null;

    // Play Launch Data
    this.bucketsToLaunch = null;
    this.topNCount = null;
    this.selectedBucket = 'A';
    this.externalSystemAuthentication = null;;
    this.destinationOrgId = null;
    this.destinationSysType = null;
    this.destinationAccountId = null;
    this.excludeItems = false;
    this.marketoProgramName = "";
    this.audienceId = "";
    this.audienceName = "";

    this.getCoverageMap = (obj) => {
        var ret = {
            errorMap : {},
            ratingModelsCoverageMap : {
                accountCount: 0,
                unscoredAccountCount: obj.accounts,
                contactCount: 0,
                unscoredContactCount: obj.contacts,
                bucketCoverageCounts: []
            }
        }
        return ret;  
    }

    this.init = function() {

        this.settings = {};
        this.launch = {};
        this.savedSegment = null;
        this.currentPlay = this.currentPlay || null;
        this.playLaunches = null;
        this.savedTalkingPoints = null;
        this.targetData = null;
        this.types = null;
        this.recommendationCounts = null;
        this.launchUnscored = false;
        this.accountsDataCount = null;

        this.settings_form = {
            play_display_name: '',
            play_description: ''
        };

        this.segment_form = {
            segment_selection: ''
        };

        this.crmselection_form = {
            crm_selection: ''
        };

        this.rating_form = {
            rating_selection: ''
        };

        this.validation = {
            name: false,
            settings: false,
            segment: true,
            rating: true,
            targets: false,
            name: true,
            crmselection: false,
            insights: false,
            preview: true,
            launch: true,
            newlaunch: true
        };

        this.barChartConfig = {
            'data': {
                'tosort': true,
                'sortBy': 'bucket_name',
                'trim': true,
                'top': 6,
            },
            'chart': {
                'header':'Value',
                'emptymsg': '',
                'usecolor': true,
                'color': '#e8e8e8',
                'mousehover': false,
                'type': 'integer',
                'showstatcount': false,
                'maxVLines': 3,
                'showVLines': false,
            },
            'vlines': {
                'suffix': ''
            },
            'columns': [{
                'field': 'num_leads',
                'label': 'Records',
                'type': 'number',
                'chart': true,
            }]
        };

        this.barChartLiftConfig = {
            'data': {
                'tosort': true,
                'sortBy': 'bucket_name',
                'trim': true,
                'top': 6,
            },
            'chart': {
                'header':'Value',
                'emptymsg': '',
                'usecolor': true,
                'color': '#e8e8e8',
                'mousehover': false,
                'type': 'decimal',
                'showstatcount': false,
                'maxVLines': 3,
                'showVLines': true,
            },
            'vlines': {
                'suffix': 'x'
            },
            'columns': [{
                    'field': 'lift',
                    'label': 'Lift',
                    'type': 'string',
                    'suffix': 'x',
                    'chart': true
                }
            ]
        };
    }

    this.init();

    this.clear = function() {
        this.init();
        this.currentPlay = null;
        CgTalkingPointStore.clear();
    }

    this.getPlays = function(cacheOnly) {
        var deferred = $q.defer();

        if (this.current.plays.length > 0) {
            deferred.resolve(this.current.plays);

            if (cacheOnly) {
                return this.current;
            }
        }

        PlaybookWizardService.getPlays().then(function(result) {
            PlaybookWizardStore.setPlays(result);
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    this.setPlays = function(plays) {
        this.current.plays = plays;
        PlaybookWizardStore.current.tileStates = {};
        angular.forEach(plays, function(play) {
            var name = play.name;
            PlaybookWizardStore.current.tileStates[name] = {
                showCustomMenu: false,
                editRating: false,
                saveEnabled: false
            };

            if(play.launchHistory && play.launchHistory.mostRecentLaunch != null && play.launchHistory.mostRecentLaunch.launchState === 'Launching'){
                PlaybookWizardStore.current.tileStates[play.name].launching = true;
                PlaybookWizardStore.checkLaunchStateInterval(play);
            }

            if(play.segment != null) {
                play.hasSegment = true;
            };
        });
    }

    this.checkLaunchStateInterval = function(play){
        PlaybookWizardStore.checkLaunchState[play.name] = $interval(function() {

            var params = {
                    playName: play.name,
                    offset: 0
                };

            PlaybookWizardStore.getPlayLaunches(params).then(function(result) {
                if(result.errorCode) {
                    $interval.cancel(PlaybookWizardStore.checkLaunchState[play.name]);
                } else if(result && result[0]) {
                    if(result[0].launchState === 'Launched' || result[0].launchState === 'Failed') {
                        $interval.cancel(PlaybookWizardStore.checkLaunchState[play.name]);
                        play.launchHistory.mostRecentLaunch.launchState = result[0].launchState;
                        PlaybookWizardStore.current.tileStates[play.name].launching == false;
                    }
                }
            });


        }, 10 * 1000);
    }

    this.cancelCheckLunch = function(){
        for(var i in PlaybookWizardStore.checkLaunchState) {
            $interval.cancel(PlaybookWizardStore.checkLaunchState[i]);
        }
        PlaybookWizardStore.checkLaunchState = {};

    }

    this.setSettings = function(obj, unset) {
        var obj = obj || {};
        for(var i in obj) {
            var key = i,
                value = obj[i];
            if(unset) {
                delete this.settings[key];
            } else {
                this.settings[key] = value;
            }
        }
    }

    this.nextSaveGeneric = function(nextState) {
        var opts = PlaybookWizardStore.settings,
            changed = false;

        if(PlaybookWizardStore.currentPlay && PlaybookWizardStore.currentPlay.name) {
            opts.name = PlaybookWizardStore.currentPlay.name;
        }

        if(PlaybookWizardStore.settings) {
            if(PlaybookWizardStore.currentPlay) {
                for(var i in PlaybookWizardStore.settings) {
                    var key = i,
                        setting = PlaybookWizardStore.settings[i];

                    if(PlaybookWizardStore.currentPlay[key] != setting) {
                        changed = true;
                        break;
                    }
                }
            } else {
                changed = true;
            }
            if(changed) {

                if(opts.ratingEngine){
                    RatingsEngineStore.getRating(opts.ratingEngine.id).then(function(result){
                        PlaybookWizardStore.setRating(result);
                    });
                } else {
                    var ratingEngine = PlaybookWizardStore.getSavedRating();
                    opts.ratingEngine = ratingEngine;
                }

                PlaybookWizardStore.savePlay(opts).then(function(play){
                    $state.go(nextState, {play_name: play.name});
                });
            } else {
                $state.go(nextState, {play_name: PlaybookWizardStore.currentPlay.name});
            }
        }
    }

    this.nextSaveAndGoto = function(nextState) {
        var opts = PlaybookWizardStore.settings,
            changed = false;

        if(PlaybookWizardStore.currentPlay && PlaybookWizardStore.currentPlay.name) {
            opts.name = PlaybookWizardStore.currentPlay.name;
        }

        if(PlaybookWizardStore.settings) {
            if(PlaybookWizardStore.currentPlay) {
                for(var i in PlaybookWizardStore.settings) {
                    var key = i,
                        setting = PlaybookWizardStore.settings[i];

                    if(PlaybookWizardStore.currentPlay[key] != setting) {
                        changed = true;
                        break;
                    }
                }
            } else {
                changed = true;
            }
            if(changed) {
                PlaybookWizardStore.savePlay(opts).then(function(play) {
                    if(play && typeof play === 'object' && play.name) {
                        if(['home.playbook.dashboard'].indexOf(nextState) > -1) {
                            $state.go(nextState);
                        } else {
                            $state.go(nextState, {play_name: play.name});
                        }
                    }
                });
            }
        }
    }

    this.nextSaveLaunch = function(nextState, opts) {
        var opts = opts || {},
            play = opts.play || PlaybookWizardStore.settings,
            launchObj = opts.launchObj || {
                bucketsToLaunch: PlaybookWizardStore.getBucketsToLaunch(),
                destinationOrgId: PlaybookWizardStore.getDestinationOrgId(),
                destinationSysType: PlaybookWizardStore.getDestinationSysType(),
                destinationAccountId: PlaybookWizardStore.getDestinationAccountId(),
                audienceName: PlaybookWizardStore.getAudienceName(),
                audienceId: PlaybookWizardStore.getAudienceId(),
                folderName: PlaybookWizardStore.getMarketoProgramName(),
                topNCount: PlaybookWizardStore.getTopNCount(),
                launchUnscored: PlaybookWizardStore.getLaunchUnscored(),
                excludeItemsWithoutSalesforceId: PlaybookWizardStore.getExcludeItems()
            },
            saveOnly = opts.saveOnly || false,
            lastIncompleteLaunchId = (PlaybookWizardStore.currentPlay.launchHistory.lastIncompleteLaunch ? PlaybookWizardStore.currentPlay.launchHistory.lastIncompleteLaunch.launchId : ''),
            lastIncompleteLaunch = opts.lastIncompleteLaunch || null;


        if(play) {
            if(play.ratingEngine){
                RatingsEngineStore.getRating(play.ratingEngine.id).then(function(result){
                    PlaybookWizardStore.setRating(result);
                });
            } else {
                var ratingEngine = PlaybookWizardStore.getSavedRating();
                play.ratingEngine = ratingEngine;
            }
            // launch saved play
            if(lastIncompleteLaunch) {
                PlaybookWizardService.saveLaunch(PlaybookWizardStore.currentPlay.name, {
                    launch_id: lastIncompleteLaunch.launchId,
                    launchObj: Object.assign({},lastIncompleteLaunch, launchObj)
                }).then(function(launch) {
                    PlaybookWizardService.saveLaunch(PlaybookWizardStore.currentPlay.name, {
                        launch_id: lastIncompleteLaunch.launchId,
                        action: 'launch'
                    }).then(function(saved) {
                        $state.go('home.playbook.dashboard.launch_job', {play_name: play.name, applicationId: saved.applicationId});
                    });
                });
            } else if(lastIncompleteLaunchId) {
                // save play
                PlaybookWizardStore.savePlay(play).then(function(play) {
                    // save launch
                    PlaybookWizardService.saveLaunch(PlaybookWizardStore.currentPlay.name, {
                        launch_id: lastIncompleteLaunchId,
                        launchObj: Object.assign({}, PlaybookWizardStore.currentPlay.launchHistory.lastIncompleteLaunch, launchObj)
                    }).then(function(saved) {
                        if(!saveOnly) {
                            // launch
                            PlaybookWizardService.saveLaunch(play.name, {
                                launch_id: lastIncompleteLaunchId,
                                action: 'launch'
                            }).then(function(launch) {
                                // after launch
                                $state.go('home.playbook.dashboard.launch_job', {play_name: play.name, applicationId: saved.applicationId});
                            });
                        } else {
                            // saved but not launched
                            $state.go('home.playbook');
                        }
                    });
                });
            } else {
                // save play
                PlaybookWizardStore.savePlay(play).then(function(play) {
                    // get launchid
                    PlaybookWizardService.saveLaunch(play.name, {
                        launchObj: launchObj
                    }).then(function(launch) {
                        var launch = launch || {};
                        // save launch
                        if(launch && !saveOnly) {
                            PlaybookWizardService.saveLaunch(PlaybookWizardStore.currentPlay.name, {
                                launch_id: launch.id,
                                action: 'launch',
                            }).then(function(saved) {
                                // after launch
                                $state.go('home.playbook.dashboard.launch_job', {play_name: play.name, applicationId: saved.applicationId});
                            });
                        } else {
                            // saved but not launched
                            $state.go('home.playbook');
                        }
                    });
                });
            }
        }
    }

    // this.nextSaveInsight = function(nextState) {
    //     if(PlaybookWizardStore.savedTalkingPoints && PlaybookWizardStore.savedTalkingPoints.length) {
    //         CgTalkingPointStore.saveTalkingPoints(PlaybookWizardStore.savedTalkingPoints).then(function(){
    //             $state.go(nextState, {play_name: PlaybookWizardStore.currentPlay.name});
    //         });
    //     } else {
    //         $state.go(nextState, {play_name: PlaybookWizardStore.currentPlay.name});
    //     }
    // }

    // *OLD*
    this.nextLaunch = function() {

        var play = PlaybookWizardStore.currentPlay,
            opts = {
                bucketsToLaunch: PlaybookWizardStore.getBucketsToLaunch(),
                topNCount: PlaybookWizardStore.getTopNCount(),
                destinationOrgId: PlaybookWizardStore.getDestinationOrgId(),
                destinationSysType: PlaybookWizardStore.getDestinationSysType(),
                destinationAccountId: PlaybookWizardStore.getDestinationAccountId(),
                audienceId: PlaybookWizardStore.getAudienceId(),
                audienceName: PlaybookWizardStore.getAudienceName(),
                folderName: PlaybookWizardStore.getMarketoProgramName(),
                excludeItems: PlaybookWizardStore.getExcludeItems(),
                launchUnscored: PlaybookWizardStore.getLaunchUnscored()
            }

        PlaybookWizardStore.launchPlay(play, opts).then(function(data) {
            $state.go('home.playbook.dashboard.launch_job', {play_name: play.name, applicationId: data.applicationId});
        });
    }

    this.setTalkingPoints = function(talkingPoints) {
        this.savedTalkingPoints = talkingPoints;
    }

    this.getTalkingPoints = function() {
        return this.savedTalkingPoints;
    }

    this.getRatings = function(active) {
        var deferred = $q.defer();
        PlaybookWizardService.getRatings(active).then(function(data) {
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    this.getRatingsCounts = function(Ratings, noSalesForceId) {
        var deferred = $q.defer(),
            ratings_ids = [],
            noSalesForceId = noSalesForceId || false;
        if(Ratings && typeof Ratings === 'object') {
            Ratings.forEach(function(value, key) {
                ratings_ids.push(value.id);
            });
            PlaybookWizardService.getRatingsCounts(ratings_ids, noSalesForceId).then(function(data) {
                deferred.resolve(data);
            });
        }
        return deferred.promise;
    }

    this.setRating = function(rating) {
        this.savedRating = rating;
    }

    this.saveRating = function(rating, play_name) {
        if (rating) {
            this.getPlay(play_name).then(function(play){
                PlaybookWizardStore.savePlay({
                    displayName: play.displayName,
                    name: play.name,
                    ratingEngine: rating,
                    launchHistory: {
                        playLaunch: {
                            bucketsToLaunch: bucketsToLaunch
                        }
                    }
                }).then(function(response){
                    PlaybookWizardStore.setSegment(segment);
                });
            });
        }
    }

    this.getSavedRating = function() {
        return this.savedRating;
    }

    this.setSegment = function(segment) {
        this.savedSegment = segment;
    }

    this.saveSegment = function(segment, play_name) {
        if (segment) {
            this.getPlay(play_name).then(function(play){
                PlaybookWizardStore.savePlay({
                    displayName: play.displayName,
                    name: play.name,
                    segment: segment.name
                }).then(function(response){
                    PlaybookWizardStore.setSegment(segment);
                });
            });
        }
    }

    this.getSavedSegment = function() {
        return this.savedSegment;
    }

    this.getValidation = function(type) {
        return this.validation[type];
    }

    this.setValidation = function(type, value) {
        this.validation[type] = value;
    }

    this.setTopNCount = function(limit) {
        this.topNCount = limit;
    }

    this.getTopNCount = function() {
        return this.topNCount;
    }

    this.setBucketsToLaunch = function(buckets) {
        this.bucketsToLaunch = buckets;
    }

    this.getBucketsToLaunch = function() {
        return this.bucketsToLaunch;
    }

    this.setDestinationOrgId = function(destinationOrgId) {
        this.destinationOrgId = destinationOrgId;
    }
    this.getDestinationOrgId = function() {
        return this.destinationOrgId;
    }

    this.setDestinationSysType = function(destinationSysType) {
        this.destinationSysType = destinationSysType;
    }
    this.getDestinationSysType = function() {
        return this.destinationSysType;
    }

    this.setDestinationAccountId = function(destinationAccountId) {
        this.destinationAccountId = destinationAccountId;
    }
    this.getDestinationAccountId = function() {
        return this.destinationAccountId;
    }

    this.setAudienceId = function(audienceId) {
        this.audienceId = audienceId;
    }
    this.getAudienceId = function() {
        return this.audienceId;
    }

    this.setAudienceName = function(audienceName) {
        this.audienceName = audienceName;
    }
    this.getAudienceName = function() {
        return this.audienceName;
    }

    this.setMarketoProgramName = function(programName) {
        this.marketoProgramName = programName;
    }

    this.getMarketoProgramName = function() {
        return this.marketoProgramName;
    }

    this.setExternalAuthentication = function(extSysAuth) {
        this.externalSystemAuthentication = extSysAuth;
    }

    this.getExternalAuthentication = function() {
        this.externalSystemAuthentication;
    }

    this.setExcludeItems = function(excludeItems) {
        this.excludeItems = excludeItems;
    }
    this.getExcludeItems = function() {
        return this.excludeItems;
    }

    this.setPlay = function(play) {
        this.currentPlay = play;
        //this.savedSegment = play.segment;
    }

    this.getCurrentPlay = function() {
        return this.currentPlay;
    }

    this.getPlay = function(play_name, nocache) {
        var deferred = $q.defer(),
            play = this.current.plays.filter(function(item) {
                return item.name == play_name;
            });

        if (play && !nocache) {
            deferred.resolve(play);
        } else {
            PlaybookWizardService.getPlay(play_name).then(function(data){
                PlaybookWizardStore.setPlay(data);
                deferred.resolve(data);
            });
        }
        return deferred.promise;
    }

    this.savePlay = function(opts) {
        var deferred = $q.defer();
        var ClientSession = BrowserStorageUtility.getClientSession();
        opts.createdBy = opts.createdBy || ClientSession.EmailAddress;
        opts.updatedBy = ClientSession.EmailAddress;

        PlaybookWizardService.savePlay(opts).then(function(data){
            PlaybookWizardStore.setPlay(data);
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    this.deletePlay = function(playName){
        var deferred = $q.defer();

        PlaybookWizardService.deletePlay(playName).then(function(result) {
            deferred.resolve(result);
            if(result === true){
                PlaybookWizardStore.setPlays(PlaybookWizardStore.current.plays.filter(function(play) {
                    return play.name != playName;
                }));
            }
        });


        return deferred.promise;
    }

    this.hasRules = function(play) {
        try {
            if (Object.keys(play.coverage).length) {
                return true;
            } else {
                return false;
            }
        } catch(err) {
            return false;
        }
    }

    this.getPlayLaunches = function(params, where) {
        var deferred = $q.defer();
        if(this.playLaunches) {
            return this.playLaunches;
        } else {
            var params = {
                playName: params.playName,
                launchStates: (params.launchStates ? params.launchStates : ''),
                sortby: params.sortby,
                descending: params.descending,
                startTimestamp: 0,
                offset: params.offset || 0,
                max: params.max || 10,
                orgId: params.orgId,
                externalSysType: params.externalSysType
            };
            PlaybookWizardService.playLaunches(params).then(function(data){
                deferred.resolve(data);
            });
            return deferred.promise;
        }

    }

    this.getPlayLaunchCount = function(params, where) {
        var deferred = $q.defer(),
            params = {
                playName: params.playName,
                launchStates: (params.launchStates ? params.launchStates : ''),
                startTimestamp: params.startTimestamp || 0,
                offset: params.offset || 0,
                orgId: params.orgId || '',
                externalSysType: params.externalSysType || ''
            };
        PlaybookWizardService.getPlayLaunchCount(params).then(function(data){
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    this.launchPlay = function(play, opts) {
        var deferred = $q.defer();
        PlaybookWizardService.launchPlay(play, opts).then(function(data){
            deferred.resolve(data);
            PlaybookWizardStore.setPlay(data);
        });
        return deferred.promise;
    }

    this.removeSegment = function(play) {
        play.segment = '';
        var deferred = $q.defer();
        PlaybookWizardService.savePlay(play).then(function(data){
            deferred.resolve(data);
            PlaybookWizardStore.setPlay(data);
        });
        return deferred.promise;
    }

    this.launchButton = function(play, launchedState) {
        var launchButton = {},
            launchButtonStates = {
                initial: {
                    label: 'Launch',
                    state: ''
                },
                Launching: {
                    label: 'Launching'
                },
                Launched: {
                    label: 'Relaunch'
                }
            },
            state = (play.launchHistory && play.launchHistory.mostRecentLaunch && play.launchHistory.mostRecentLaunch.launchState ? play.launchHistory.mostRecentLaunch.launchState : null);

        state = (play.launchHistory.lastCompletedLaunch ? 'Launched' : state);

        launchButton.state = launchedState || state;

        if(launchedState !== 'Failed' && state && launchButtonStates[state]) {
            launchButton.label = launchButtonStates[state].label;
        } else {
            if((state === 'Failed' ||  launchedState === 'Failed') && play.launchHistory.playLaunch) {
                launchButton.label = launchButtonStates.Launched.label;
            } else {
                launchButton.label = launchButtonStates.initial.label;
            }
        }

        return launchButton;
    }

    this.getLaunchedStatus = function(play) {
        var launchedState = (play.launchHistory && play.launchHistory.playLaunch && play.launchHistory.playLaunch.launchState ? play.launchHistory.playLaunch.launchState : null),
            hasLaunched = (launchedState === 'Launched' ? true : false),
            //hasLaunchHistory = (play.launchHistory.mostRecentLaunch ||  play.launchHistory.lastCompletedLaunch || play.launchHistory.lastIncompleteLaunch ? true : false);
            hasLaunchHistory = ((play.launchHistory && play.launchHistory.mostRecentLaunch && ['Launching','Launched','Failed'].indexOf(play.launchHistory.mostRecentLaunch.launchState) !== -1 || play.launchHistory && play.launchHistory.lastCompletedLaunch && play.launchHistory.lastCompletedLaunch.launchState) ? true : false);
        return {
            hasLaunchHistory: hasLaunchHistory,
            launchedState: launchedState,
            hasLaunched: hasLaunched,
        };
    }

    this.setTargetData = function(targetData) {
        this.targetData = targetData;
    }
    this.getTargetData = function(){
        return this.targetData;
    };

    this.setTalkingPoints = function(talkingPoints) {
        this.savedTalkingPoints = talkingPoints;
    }

    this.getTalkingPoints = function() {
        return this.savedTalkingPoints;
    }

    this.setTypes = function(types) {
        this.types = types;
    }

    this.getTypes = function(params) {
        var deferred = $q.defer();

        PlaybookWizardService.getTypes().then(function(data){
            PlaybookWizardStore.setTypes(data);
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    this.setGroups = function(groups) {
        this.groups = groups;
    }

    this.getGroups = function(params) {
        var deferred = $q.defer();

        PlaybookWizardService.getGroups().then(function(data){
            PlaybookWizardStore.setGroups(data);
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    this.setRecommendationCounts = function(recommendationCounts) {
        this.recommendationCounts = recommendationCounts;
    }

    this.getRecommendationCounts = function() {
        return this.recommendationCounts;
    }

    this.setLaunchUnscored = function(bool) {
        this.launchUnscored = bool;
    }

    this.getLaunchUnscored = function(bool) {
        return this.launchUnscored;
    }

    this.getAccountsCount = function(query) {
        if(query && query.page_filter) {
            delete query.page_filter;
        }
        var deferred = $q.defer();

        PlaybookWizardService.getAccountsCount(query).then(function(result){
            deferred.resolve(result);
        });
        return deferred.promise;
    }

    this.setAccountsDataCount = function(count) {
        this.accountsDataCount = count;
    }

    this.isExternallyAuthenticatedOrg = function() {
        return this.externalSystemAuthentication && this.externalSystemAuthentication.trayAuthenticationId;
    }

    this.launchAccountsCoverage = function(play_name, opts) {
        var deferred = $q.defer(),
            play_name = play_name || $stateParams.play_name,
            sendEngineId = opts.sendEngineId,
            getExcludeItems = opts.getExcludeItems || PlaybookWizardStore.getExcludeItems(),
            getDestinationAccountId = opts.getDestinationAccountId ||  PlaybookWizardStore.getDestinationAccountId();

        PlaybookWizardStore.getPlay(play_name, true).then(function (data) {
            if (data.ratingEngine && data.ratingEngine.id) {
                var engineId = data.ratingEngine.id,
                    engineIdObject = [{ id: engineId }],
                    getExcludeItems = getExcludeItems,
                    getSegmentsOpts = {
                        loadContactsCount: true,
                        loadContactsCountByBucket: true
                    };

                if (getExcludeItems) {
                    getSegmentsOpts.lookupId = getDestinationAccountId;
                    getSegmentsOpts.restrictNullLookupId = true;
                }

                var segmentName = PlaybookWizardStore.getCurrentPlay().targetSegment.name;
                PlaybookWizardService.getRatingSegmentCounts(segmentName, [engineId], getSegmentsOpts).then(function (result) {
                    var accountsCoverage = {};
                    accountsCoverage.errorMap = result.errorMap;
                    accountsCoverage.ratingModelsCoverageMap = result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]];
                    if(sendEngineId) {
                        accountsCoverage.engineId = engineId;
                    }
                    deferred.resolve(accountsCoverage);
                });
            } else {
                var segment = PlaybookWizardStore.getCurrentPlay().targetSegment;
                if (getExcludeItems) {
                    var accountId = PlaybookWizardStore.crmselection_form.crm_selection.accountId,
                        template = {
                            account_restriction: {
                                restriction: {
                                    logicalRestriction: {
                                        operator: "AND",
                                        restrictions: []
                                    }
                                }
                            },
                            page_filter: {
                                num_rows: 10,
                                row_offset: 0
                            }
                        };
                    template.account_restriction.restriction.logicalRestriction.restrictions.push(segment.account_restriction.restriction);
                    template.account_restriction.restriction.logicalRestriction.restrictions.push({
                        bucketRestriction: {
                            attr: 'Account.' + accountId,
                            bkt: {
                                Cmp: 'IS_NOT_NULL',
                                Id: 1,
                                ignored: false,
                                Vals: []
                            }
                        }
                    });
                    QueryStore.getEntitiesCounts(template).then(function (result) {
                        deferred.resolve(
                            PlaybookWizardStore.getCoverageMap({
                                accounts: result.Account || 0,
                                contacts: result.Contact || 0,
                            }));
                    });
                }
                else {
                    deferred.resolve(
                        PlaybookWizardStore.getCoverageMap({
                            accounts: segment.accounts || 0,
                            contacts: segment.contacts || 0
                        }));
                }
            }
        });
        return deferred.promise;
    }

})
.service('PlaybookWizardService', function($q, $http, $state, $timeout) {
    this.host = '/pls'; //default

    this.getPlays = function() {
        var deferred = $q.defer(),
            result,
            url = '/pls/play' + '?shouldLoadCoverage=true';
        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.deletePlay = function(playName) {
        var deferred = $q.defer(),
            result,
            url = '/pls/play/' + playName;
        $http({
            method: 'DELETE',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.setHost = function(value) {
        this.host = value;
    }

    this.getPlay = function(play_name) {
        var deferred = $q.defer(),
            play_name_url = (play_name ? '/' + play_name : '');
        $http({
            method: 'GET',
            url: this.host + '/play' + play_name_url
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.savePlay = function(opts) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/play',
            data: opts
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.saveLaunch = function(play_name, opts) {
        var deferred = $q.defer(),
            opts = opts || {},
            launch_id = opts.launch_id || '',
            action = opts.action || '',
            launchObj = opts.launchObj || '';

        $http({
            method: 'POST',
            url: this.host + '/play/' + play_name + '/launches' + (launch_id ? '/' + launch_id : '') + (action ? '/' + action : ''),
            data: launchObj
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getPlayLaunchCount = function(params) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/play/launches/dashboard/count',
            params: params
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.playLaunches = function(params) {

        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: this.host + '/play/launches/dashboard/',
            params: params
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.launchPlay = function(play, opts) {
        var deferred = $q.defer(),
            play_name = play.name,
            bucketsToLaunch = opts.bucketsToLaunch,
            topNCount = opts.topNCount,
            destinationOrgId = opts.destinationOrgId,
            destinationSysType = opts.destinationSysType,
            destinationAccountId = opts.destinationAccountId,
            audienceId = opts.audienceId,
            audienceName = opts.audienceName,
            folderName = opts.programName,
            excludeItems = opts.excludeItems,
            launchUnscored = opts.launchUnscored;
        $http({
            method: 'POST',
            url: this.host + '/play/' + play_name + '/launches',
            data: {
                launch_state: 'Launching',
                bucketsToLaunch: bucketsToLaunch,
                topNCount: topNCount,
                destinationOrgId: destinationOrgId,
                destinationSysType: destinationSysType,
                destinationAccountId: destinationAccountId,
                excludeItemsWithoutSalesforceId: excludeItems,
                launchUnscored: launchUnscored,
                audienceId: audienceId,
                audienceName: audienceName,
                folderName: folderName
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getRatings = function(opts) {
        var deferred = $q.defer(),
            opts = opts;

        $http({
            method: 'GET',
            params: {
                type: opts.type || null, //'RULE_BASED'
                status: (opts.active ? 'ACTIVE' : ''),
                'publishedratingsonly': 'true'
            },
            url: this.host + '/ratingengines',
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getRatingsCounts = function(ratings, noSalesForceId) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/ratingengines/coverage',
            data: {
                ratingEngineIds: ratings,
                restrictNotNullSalesforceId: noSalesForceId,

            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getRatingSegmentCounts = function(segmentName, ratings, opts) {
        var deferred = $q.defer(),
            opts = opts || {};
        $http({
            method: 'POST',
            url: this.host + '/ratingengines/coverage/segment/' +  segmentName,
            data: {
                ratingEngineIds: ratings,
                loadContactsCountByBucket: opts.loadContactsCountByBucket,
                loadContactsCount: opts.loadContactsCount,
                lookupId: opts.lookupId,
                restrictNullLookupId: opts.restrictNullLookupId,
                applyEmailFilter: opts.applyEmailFilter || false
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getLookupCounts = function(engineId, accountId) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/ratingengines/coverage',
            data: {
                ratingIdLookupColumnPairs:[
                    {
                        responseKeyId: accountId || null,
                        ratingEngineId: engineId || null,
                        lookupColumn: accountId || null
                    }
                ]
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getTargetData = function(engineId, query) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/ratingengines/' + engineId + '/entitypreview',
            params: query
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getTargetCount = function(engineId, query) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/ratingengines/' + engineId + '/entitypreview/count',
            params: query
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getAccountsData = function(query) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/accounts/data',
            data: query
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    };

    this.getAccountsCount = function(query) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/accounts/count',
            data: query
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    };

    this.getTypes = function(engineId, query) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: '/pls/playtypes',
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getGroups = function(engineId, query) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: '/pls/playgroups',
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getTenantDropboxId = function() {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: '/pls/dropbox/summary',
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result.DropBox);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.getTrayUser = function(userName) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: '../tray/user?userName=' + userName
        }).then(
            function onSuccess(response) {
                var result = response.data
                deferred.resolve(result.id);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }


    this.getTrayAuthorizationToken = function(userId) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: '../tray/authorize?userId=' + userId
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result.token);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }


    this.getMarketoPrograms = function(trayAuthenticationId, useraccesstoken) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: '../tray/marketo/programs?trayAuthenticationId=' + trayAuthenticationId,
            headers: {
                'useraccesstoken': useraccesstoken
            }
        }).then(
            function onSuccess(response) {
                deferred.resolve(response.data);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }


    this.getMarketoStaticLists = function(trayAuthenticationId, useraccesstoken, programName) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: '../tray/marketo/staticlists',
            headers: {
                useraccesstoken: useraccesstoken
            },
            params: {
                trayAuthenticationId: trayAuthenticationId,
                programName: programName
            }
        }).then(
            function onSuccess(response) {
                deferred.resolve(response.data);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }
});
