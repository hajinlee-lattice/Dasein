angular.module('lp.playbook')
.service('PlaybookWizardStore', function($q, $state, $stateParams,  $interval, PlaybookWizardService, CgTalkingPointStore, BrowserStorageUtility, RatingsEngineStore){
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
    this.destinationOrgId = null;
    this.destinationSysType = null;
    this.destinationAccountId = null;
    this.excludeItems = false;

    this.init = function() {

        this.settings = {};
        this.savedSegment = null;
        this.currentPlay = this.currentPlay || null;
        this.playLaunches = null;
        this.savedTalkingPoints = null;
        this.targetData = null;

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
            settings: false,
            rating: true,
            targets: true,
            crmselection: false,
            insights: false,
            preview: true,
            launch: true
        };

        this.barChartConfig = {
            'data': {
                'tosort': true,
                'sortBy': 'bucket_name',
                'trim': true,
                'top': 5,
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
                'top': 5,
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

            if(play.launchHistory.mostRecentLaunch != null && play.launchHistory.mostRecentLaunch.launchState === 'Launching'){
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

    this.setSettings = function(obj) {
        var obj = obj || {};
        for(var i in obj) {
            var key = i,
                value = obj[i];
            this.settings[key] = value;
        }
    }
    
    


    this.nextSaveGeneric = function(nextState) {
        var changed = false,
            opts = PlaybookWizardStore.settings;

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

    // this.nextSaveInsight = function(nextState) {
    //     if(PlaybookWizardStore.savedTalkingPoints && PlaybookWizardStore.savedTalkingPoints.length) {
    //         CgTalkingPointStore.saveTalkingPoints(PlaybookWizardStore.savedTalkingPoints).then(function(){
    //             $state.go(nextState, {play_name: PlaybookWizardStore.currentPlay.name});
    //         });
    //     } else {
    //         $state.go(nextState, {play_name: PlaybookWizardStore.currentPlay.name});
    //     }
    // }

    this.nextLaunch = function() {

        var play = PlaybookWizardStore.currentPlay,
            opts = {
                bucketsToLaunch: PlaybookWizardStore.getBucketsToLaunch(),
                topNCount: PlaybookWizardStore.getTopNCount(),
                destinationOrgId: PlaybookWizardStore.getDestinationOrgId(),
                destinationSysType: PlaybookWizardStore.getDestinationSysType(),
                destinationAccountId: PlaybookWizardStore.getDestinationAccountId(),
                excludeItems: PlaybookWizardStore.getExcludeItems()
            }

        console.log(opts);

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

    this.getPlayLaunches = function(params) {
        var deferred = $q.defer();
        if(this.playLaunches) {
            return this.playLaunches;
        } else {

            var params = {
                playName: params.playName,
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
    this.getPlayLaunchCount = function(params) {
        var deferred = $q.defer(),
            params = {
                playName: params.playName,
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
                    label: 'Re-Launch Now'
                }
            },
            state = (play.launchHistory && play.launchHistory.mostRecentLaunch && play.launchHistory.mostRecentLaunch.launchState ? play.launchHistory.mostRecentLaunch.launchState : null);

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
            hasLaunched = (launchedState === 'Launched' ? true : false);
        return {
            launchedState: launchedState,
            hasLaunched: hasLaunched
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

                console.log(response);

                var result = response.data;
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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

    var canceler = $q.defer();

    this.playLaunches = function(params) {

        canceler.resolve("cancelled");
        canceler = $q.defer();

        $http({
            method: 'GET',
            url: this.host + '/play/launches/dashboard/',
            params: params
        }).then(
            function onSuccess(response) {
                var result = response.data;
                if (result != null && result !== "") {
                    result = response.data;
                    canceler.resolve(result);
                } else {
                    
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    canceler.resolve(errorMsg);
                }

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                canceler.reject(errorMsg);
            }
        );
        return canceler.promise;
    }

    this.launchPlay = function(play, opts) {

        var deferred = $q.defer(),
            play_name = play.name,
            bucketsToLaunch = opts.bucketsToLaunch,
            topNCount = opts.topNCount,
            destinationOrgId = opts.destinationOrgId,
            destinationSysType = opts.destinationSysType,
            destinationAccountId = opts.destinationAccountId,
            excludeItems = opts.excludeItems;

        console.log(destinationOrgId, destinationSysType, destinationAccountId);

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
                excludeItemsWithoutSalesforceId: excludeItems
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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
                if (result != null && result !== "") {
                    result = response.data;
                    deferred.resolve(result);
                } else {
                    // var errors = result.Errors;
                    // var result = {
                    //         success: false,
                    //         errorMsg: errors[0]
                    //     };
                    // deferred.resolve(result.errorMsg);

                    if (!response.data) {
                        response.data = {};
                    }

                    var errorCode = response.data.errorCode || 'Error';
                    var errorMsg = response.data.errorMsg || 'unspecified error.';

                    deferred.resolve(errorMsg);
                }

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
