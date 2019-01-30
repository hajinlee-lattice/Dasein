import DateUtils from '../../date-utils/date-utils.js';
    
angular.module('lp.import.wizard.customfields')
.component('dateFieldComponent', {
    templateUrl: 'app/import/content/customfields/date-options.component.html',
    bindings: {
        field: '=',
        disabeld: '=',
        updateType: '&'
      },
    controller: function ($state, $ngRedux) {
        var vm = this;
        this.redux = $state.get('home.import').data.redux;
        this.dateFormat = DateUtils.getDateFormat(this.field);
        this.timeFormat = DateUtils.getTimeFormat(this.field);
        this.timezone = DateUtils.getTimezone(this.field);
        this.dateFormats = this.redux.store.dateFormats;
        this.timeFormats = this.redux.store.timeFormats;
        this.timeZones = this.redux.store.timezones;
       
        this.$onInit = function() {

        };

        this.changeDateFormat = () => {
            DateUtils.setDateFormat(this.field, this.dateFormat); 
            if(this.updateType){
                this.updateType({fieldMapping: this.field});
            }
        };
        this.changeTimeFormat = () => {
            DateUtils.setTimeFormat(this.field,this.timeFormat);
            if(this.updateType){
                this.updateType({fieldMapping: this.field});
            }
        }
        this.changeTimezone = () => {
            DateUtils.setTimezone(this.field, this.timezone);
            if(this.updateType){
                this.updateType({fieldMapping: this.field});
            }
        }
        

    }
});