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
    controller: function ($state, ImportUtils) {
        var vm = this;
        this.redux = $state.get('home.import').data.redux;
        
        this.dateFormat = getDateFormat(this.field);
        this.timeFormat = getTimeFormat(this.field);
        this.timezone = getTimezone(this.field);
        this.dateFormats = [
            {value: 'MM/DD/YYYY'},
            {value: 'MM-DD-YYYY'},
            {value: 'MM.DD.YYYY'},
            {value: 'DD/MM/YYYY'},
            {value: 'DD-MM-YYYY'},
            {value: 'DD.MM.YYYY'},
            {value: 'YYYY/MM/DD'},
            {value: 'YYYY-MM-DD'},
            {value: 'YYYY.MM.DD'},
            {value: 'MM/DD/YY'},
            {value: 'MM-DD-YY'},
            {value: 'MM.DD.YY'},
            {value: 'DD/MM/YY'},
            {value: 'DD-MM-YY'},
            {value: 'DD.MM.YY'}
        ];
        this.timeFormats = [
            {value: '00:00:00 12H'},
            {value: '00-00-00 12H'},
            {value: '00 00 00 12H'},
            {value: '00:00:00 24H'},
            {value: '00-00-00 24H'},
            {value: '00 00 00 24H'}
        ];
        this.timeZones = [
            {value: 'UTC-8 America/Los Angeles'}
        ];

        this.$onInit = function() {
            // console.log('TEST', this.field);

        };

        this.changeDateFormat = () => {
            setDateFormat(this.field, this.dateFormat); 
            // console.log("UPDATED ",this.field);
            if(this.updateType){
                this.updateType({fieldMapping: this.field});
            }
        };
        this.changeTimeFormat = () => {
            setTimeFormat(this.field,this.timeFormat);
            // console.log("UPDATED ",this.field);
            if(this.updateType){
                this.updateType({fieldMapping: this.field});
            }
        }
        this.changeTimezone = () => {
            setTimezone(this.field, this.timezone);
            // console.log("UPDATED ",this.field);
            if(this.updateType){
                this.updateType({fieldMapping: this.field});
            }
        }
        

    }
});