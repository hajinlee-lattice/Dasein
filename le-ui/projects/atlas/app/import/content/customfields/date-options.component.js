import {isOnlyDateMandatory, getDateFormat, setDateFormat,
    getTimeFormat, getTimezone, setTimeFormat, setTimezone} from '../../date-utils/date-utils';
    
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
        this.dateFormat = getDateFormat(this.field);
        this.timeFormat = getTimeFormat(this.field);
        this.timezone = getTimezone(this.field);
        this.dateFormats = this.redux.store.dateFormats;
        this.timeFormats = this.redux.store.timeFormats;
        this.timeZones = this.redux.store.timezones;
       
        this.$onInit = function() {

        };

        this.changeDateFormat = () => {
            setDateFormat(this.field, this.dateFormat); 
            if(this.updateType){
                this.updateType({fieldMapping: this.field});
            }
        };
        this.changeTimeFormat = () => {
            setTimeFormat(this.field,this.timeFormat);
            if(this.updateType){
                this.updateType({fieldMapping: this.field});
            }
        }
        this.changeTimezone = () => {
            setTimezone(this.field, this.timezone);
            if(this.updateType){
                this.updateType({fieldMapping: this.field});
            }
        }
        

    }
});