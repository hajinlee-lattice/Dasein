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
            timezone: '=',
            mapping: '=',
            validate: '<'
        },
        controller: function ($state, $scope) {
            let self = this;
            this.daterequired = false;
            this.timezonerequired = false;
            this.$doCheck = function(){
                if((this.mapping && this.mapping != null && this.mapping != "" && !this.dateformat) || (this.validate && !this.dateformat)){
                    this.daterequired = true;
                }else {
                    this.daterequired = false;
                }
                if((this.mapping && this.mapping != null && this.mapping != "" && this.timeformat && this.timeformat != null && this.timeformat !="")|| (this.validate && this.timeformat && this.timeformat != null && this.timeformat !="") ){
                    this.timezonerequired = true;
                }else{
                    this.timezonerequired = false;
                }
            }.bind(this);

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