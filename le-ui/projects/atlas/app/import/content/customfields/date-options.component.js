import DateUtils from '../../date-utils/date-utils.js';

angular.module('lp.import.wizard.customfields')
    .component('dateFieldComponent', {
        templateUrl: 'app/import/content/customfields/date-options.component.html',
        bindings: {
            field: '=',
            ignore: '=',
            update: '&',
            tooltiptxt: '@',
            dateformat: '=',
            timeformat: '=',
            timezone: '='
        },
        controller: function ($state, $scope) {
            let self = this;
            
            this.$onInit = function () {
                this.redux = $state.get('home.import').data.redux;
                this.dateFormats = this.redux.store.dateFormats;
                this.timeFormats = this.redux.store.timeFormats;
                this.timeZones = this.redux.store.timezones;
                this.formerTemplates = this.field.fromExistingTemplate;
            };
            this.getTooltip = () => {
                return this.tooltiptxt;
            }
          
            this.updateFormats = () => {
                    self.update({ formats: { field: this.field, dateformat: this.dateformat, timeformat: this.timeformat, timezone: this.timezone } }); 
            };
        }
    });