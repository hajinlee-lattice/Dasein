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
        play_name: '',
        play_description: ''
    }

    this.segment_form = {
        segment_selection: ''
    }

    this.rating_form = {
        rating_selection: ''
    }

    this.savedSegment = this.savedSegment || null;

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
})
.service('PlaybookWizardService', function($q, $http, $state) {
});
