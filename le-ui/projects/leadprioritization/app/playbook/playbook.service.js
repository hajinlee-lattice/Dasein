angular.module('lp.playbook')
.service('PlaybookWizardStore', function($q, $state, PlaybookWizardService){
    var PlaybookWizardStore = this;

    this.settings = this.settings || {};
    this.rating = this.rating || {};
    this.savedSegment = this.savedSegment || null;
    this.currentPlay = this.currentPlay || null;


    this.clear = function() {
        this.settings = {};
        this.rating = {};
        this.savedSegment = null;
        this.currentPlay = null;
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
    }

    this.validation = {
        settings: false,
        segment: false,
        rating: false,
        targets: true,
        insights: true,
        preview: true,
        launch: true
    };

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

    this.setRating = function(rating) {
        this.rating = rating;
    }

    this.getRating = function() {
        return this.rating;
    }

    this.getRatings = function() {
        var data = [],
            total = 5 ;//Math.floor(Math.random() * 10 + 3); // 3 - 10

        for (var i=0; i<total; i++) {
            var tmp = {
                Name: 'Rating Name ' + (i + 1)
            };

            data.push(tmp);
        }

        return data;
    };

    this.setSegment = function(segment) {
        this.savedSegment = segment;
    }

    this.saveSegment = function(segment, play_name) {
        if (segment) {
            this.getPlay(play_name).then(function(play){
                PlaybookWizardStore.savePlay({
                    display_name: play.display_name,
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
        this.savedSegment = play.segment;
    }

    this.getCurrentPlay = function() {
        return this.currentPlay;
    }

    this.getPlay = function(play_name) {
        var deferred = $q.defer();
        if(this.currentPlay && play_name) {
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
        PlaybookWizardService.savePlay(opts).then(function(data){
            deferred.resolve(data);
            PlaybookWizardStore.setPlay(data);
        });
        return deferred.promise;
    }

    this.nextLaunch = function() {
        PlaybookWizardStore.launchPlay(PlaybookWizardStore.currentPlay).then(function(data) {
        });
    }

    this.launchPlay = function(play) {
        var deferred = $q.defer();
        console.log(play);
        PlaybookWizardService.launchPlay(play).then(function(data){
            deferred.resolve(data);
            PlaybookWizardStore.setPlay(data);
        });
        return deferred.promise;
    }
})
.service('PlaybookWizardService', function($q, $http, $state) {
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

    this.launchPlay = function(play) {
        var deferred = $q.defer(),
            play_name = play.name;
        $http({
            method: 'POST',
            url: this.host + '/play/' + play_name + '/launches',
            data: {
                name: play.name,
                description: play.description,
                segment: play.segment
            }
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }
});
