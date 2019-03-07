import DateUtils from '../../date-utils/date-utils.js';

angular.module('lp.import.wizard.customfields')
    .component('dateFieldComponent', {
        templateUrl: 'app/import/content/customfields/date-options.component.html',
        bindings: {
            field: '=',
            disabeld: '=',
            updateType: '&',
            tooltiptxt: '@'
        },
        controller: function ($state, $scope, ImportUtils) {

            this.$onInit = function () {
                this.redux = $state.get('home.import').data.redux;
                this.dateFormats = this.redux.store.dateFormats;
                this.timeFormats = this.redux.store.timeFormats;
                this.timeZones = this.redux.store.timezones;
                this.dateFormat = DateUtils.getDateFormat(this.field, this.dateFormats);
                this.timeFormat = DateUtils.getTimeFormat(this.field, this.timeFormats);
                this.timezone = DateUtils.getTimezone(this.field, this.timeZones);
                this.formerTemplates = this.field.fromExistingTemplate;
            };
            this.getTooltip = () => {
                return this.tooltiptxt;
            }
            this.setValues = (field, dateFormat, timeFormat, timezone) => {
                field.dateFormatString = dateFormat;
                field.timeFormatString = timeFormat;
                field.timezone = timezone;

            }
            this.changeDateFormat = (value) => {
                let copy = angular.copy(this.field);
                this.setValues(copy, value, this.timeFormat, this.timezone);

                if (this.updateType) {
                    this.updateType({ fieldMapping: copy });
                }
                setTimeout(() => {
                    $scope.$apply();
                },0);
            };
            this.changeTimeFormat = (value) => {
                let copy = angular.copy(this.field);
                this.setValues(copy, this.dateFormat, value, this.timezone);
                if (this.updateType) {
                    this.updateType({ fieldMapping: copy });
                }
                setTimeout(() => {
                    $scope.$apply();
                },0);
            }
            this.changeTimezone = (value) => {
                let copy = angular.copy(this.field);
                this.setValues(copy, this.dateFormat, this.timeFormat, value);
                if (this.updateType) {
                    this.updateType({ fieldMapping: copy });
                }
                setTimeout(() => {
                    $scope.$apply();
                },0);
            }


        }
    });