angular.module('lp.playbook')
.service('PlaybookWizardStore', function($q, $state, $stateParams, PlaybookWizardService, CgTalkingPointStore, BrowserStorageUtility){
    var PlaybookWizardStore = this;
    
    this.current = {
        plays: []
    };

    this.init = function() {
        this.settings = {};
        this.savedRating = null;
        this.savedSegment = null;
        this.currentPlay = this.currentPlay || null;
        this.playLaunches = null;
        this.savedTalkingPoints = null;

        this.settings_form = {
            play_display_name: '',
            play_description: ''
        }

        this.segment_form = {
            segment_selection: ''
        }

        this.rating_form = {
            rating_selection: ''
        }

        this.validation = {
            settings: false,
            rating: true,
            targets: false,
            insights: false,
            preview: true,
            launch: true
        }
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
                PlaybookWizardStore.savePlay(opts).then(function(play){
                    $state.go(nextState, {play_name: play.name});
                });
            } else {
                $state.go(nextState, {play_name: PlaybookWizardStore.currentPlay.name});
            }
        }
    }

    this.nextSaveInsight = function(nextState) {
        if(PlaybookWizardStore.savedTalkingPoints && PlaybookWizardStore.savedTalkingPoints.length) {
            CgTalkingPointStore.saveTalkingPoints(PlaybookWizardStore.savedTalkingPoints).then(function(){
                $state.go(nextState, {play_name: PlaybookWizardStore.currentPlay.name});
            });
        } else {
            $state.go(nextState, {play_name: PlaybookWizardStore.currentPlay.name});
        }
    }

    this.nextLaunch = function() {
        var play = PlaybookWizardStore.currentPlay;
        PlaybookWizardStore.launchPlay(play).then(function(data) {
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
                    ratingEngine: rating
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

    this.setPlay = function(play) {
        this.currentPlay = play;
        //this.savedSegment = play.segment;
    }

    this.getCurrentPlay = function() {
        return this.currentPlay;
    }

    this.getPlay = function(play_name, nocache) {
        var deferred = $q.defer();
        if(this.currentPlay && play_name && !nocache) {
            deferred.resolve(this.currentPlay);
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
            deferred.resolve(data);
            PlaybookWizardStore.setPlay(data);
        });
        return deferred.promise;
    }

    this.getPlayLaunches = function(params) {
        var deferred = $q.defer();
        if(this.playLaunches) {
            return this.playLaunches;
        } else {
            PlaybookWizardService.playLaunches(params).then(function(data){
                deferred.resolve(data);
            });
            return deferred.promise;
        }
        
    }
    this.launchPlay = function(play) {
        var deferred = $q.defer();
        PlaybookWizardService.launchPlay(play).then(function(data){
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

    this.getTargetData = function(engineId, query){ 
        var deferred = $q.defer();

        PlaybookWizardService.getTargetData(engineId, query).then(function(response) {
            deferred.resolve(response);
        });
        return deferred.promise;
    };

})
.service('PlaybookWizardService', function($q, $http, $state, $timeout) {
    this.host = '/pls'; //default

    this.getPlays = function() {

        var deferred = $q.defer(),
            result,
            url = '/pls/play';

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
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
                result = response.data;
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


    this.setHost = function(value) {
        this.host = value;
    }

    this.getPlay = function(play_name) {
        var deferred = $q.defer(),
            play_name_url = (play_name ? '/' + play_name : '');
        $http({
            method: 'GET',
            url: this.host + '/play' + play_name_url
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.savePlay = function(opts) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/play',
            data: opts
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.playLaunches = function(params) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/play/launches/dashboard/',
            params: {
                playName: params.playName,
                startTimestamp: 0,
                offset: params.offset || 0,
                max: 10
            }
        }).then(function(response){
            deferred.resolve(response.data);
        }, function(response) {
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.launchPlay = function(play) {
        var deferred = $q.defer(),
            play_name = play.name;
        $http({
            method: 'POST',
            url: this.host + '/play/' + play_name + '/launches',
            data: {
                launch_state: 'Launching'
            }
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.getRatings = function(active) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/ratingengines' + (active ? '?status=ACTIVE' : ''),
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
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

    this.getRatingsCounts = function(ratings, noSalesForceId) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/ratingengines/coverage',
            data: {
                ratingEngineIds: ratings,
                restrictNotNullSalesforceId: noSalesForceId
            }
        }).then(function(response) {
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.getTargetData = function(engineId, query) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/ratingengines/' + engineId + '/entitypreview',
            params: query
        }).then(function(response) {
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

});
