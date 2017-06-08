angular.module('lp.playbook')
.service('PlaybookWizardStore', function($q, PlaybookWizardService){
    var PlaybookWizardStore = this;

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

    this.nextSettings = function(nextState) {
        console.log('nextSettings');
    }

    this.savedSegment = this.savedSegment || null;

    this.currentPlay = this.currentPlay || null;

    this.getRatings = function() {
        var data = [],
            total = Math.floor(Math.random() * 10 + 3); // 3 - 10

        for (var i=0; i<total; i++) {
            var tmp = {
                Name: 'Rating Name ' + (i + 1)
            };

            data.push(tmp);
        }

        return data;
    };

    this.saveSegment = function(segment) {
        if (segment) {
            this.savedSegment = segment;
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

    this.getPlay = function(play_name) {
        var deferred = $q.defer();
        PlaybookWizardService.getPlay(play_name).then(function(data){
            deferred.resolve(data);
            this.currentPlay = data;
        });
        return deferred.promise;
    }

    this.savePlay = function(opts) {
        var deferred = $q.defer();
        PlaybookWizardService.savePlay(opts).then(function(data){
            deferred.resolve(data);
        });
        return deferred.promise;
    }
})
.service('PlaybookWizardService', function($q, $http, $state) {
    this.host = '/pls'; //default

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
        console.log('service saveplay');
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
});
