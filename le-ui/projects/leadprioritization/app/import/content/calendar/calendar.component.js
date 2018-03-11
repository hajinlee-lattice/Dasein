angular.module('lp.import.calendar', [])
.controller('ImportWizardCalendar', function(
    $state, $stateParams, $scope, $timeout, NumberUtility, ResourceUtility, ImportWizardStore, ImportWizardService, Calendar, FieldDocument
) {
    var vm = this,
        preventUnload = true,
        year = new Date().getFullYear(),
        months = ['January','February','March','April','May','June','July','August','September','October','November','December'],
        weekdays = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'],
        picker;

    if(preventUnload) {
        window.onbeforeunload = function(event) {
            var warning = 'Changes you made may not be saved.  Are you sure?';
            event.returnValue = warning;
            return warning;
        };
    }

    $scope.$on("$destroy", function(){
        window.onbeforeunload = null;
    });

    angular.extend(vm, {
        calendar: Calendar,
        selectedDate: '01-01',
        selectedQuarter: '1',
        mode: '',
        calendarStore: {}
    });

    function initDatePicker() {
        var DATE_FORMAT = 'DD-MM-YYYY',
            selectedDateObj = {},
            field = document.getElementById('datepicker');

        picker = new Pikaday({
            format: DATE_FORMAT,
            theme: 'file-import-datepicker',
            minDate:  new Date("01-01-"+year),
            maxDate: new Date("12-31-"+year),
            i18n: { // if you have any of these you have to have them all or it will error out
                previousMonth: 'Previous Month',
                nextMonth: 'Next Month',
                months: months,
                weekdays: weekdays,
                weekdaysShort : ['S','M','T','W','T','F','S']
            },
            onSelect: function(date) {
                selectedDateObj = {
                        mode: "STARTING_DATE",
                        startingDate: months[(date.getMonth() + 1)].substring(0,3).toUpperCase() + "-" + NumberUtility.PadNumber(date.getDate(),2),
                        evaluationYear: date.getFullYear(), 
                        longerMonth: vm.selectedQuarter,
                        
                    };
                vm.setCalendar(selectedDateObj);
            }
        });
        field.parentNode.insertBefore(picker.el, field.nextSibling);
        picker.setDate('01-01-'+year, true); // second param prevents onSelect callback
    }

    vm.init = function() {
        if(preventUnload && !FieldDocument) {
            $state.go('home.import.entry.product_hierarchy');
            return false;
        }
        $timeout(initDatePicker, 0);
    };

    vm.selectDate = function(date) {
        vm.selectedDate = date;
        picker.gotoDate(new Date(date+'-'+year)); 
    }

    vm.selectQuarter = function(quarter) {
        vm.selectedQuarter = quarter;
        if(vm.calendar && vm.calendar.longerMonth) {
            vm.calendar.longerMonth = vm.selectedQuarter;
            ImportWizardService.validateCalendar(vm.calendar).then(function(result) {
                vm.note = result.note;
            });
        }
    }

    vm.changeMode = function(mode) {
        if(vm.mode !== mode) {
            vm.mode = mode;
            vm.note = '';
            if(vm.calendarStore[mode]) {
                vm.setCalendar(vm.calendarStore[mode]);
            }
        }
    }

    vm.changeNth = function(nthMapping) {
        var nth = nthMapping.nth || '',
            day = nthMapping.day || '',
            month = nthMapping.month || '',
            selectedNthObj = {
                mode: "STARTING_DAY",
                startingDay: nth + "-" + day + "-" + month,
                longerMonth: vm.selectedQuarter
            }

        if(nth && day && month) {
            vm.setCalendar(selectedNthObj)
        }
    }

    vm.setCalendar = function(obj) {
        ImportWizardStore.setCalendar(obj);
        ImportWizardStore.getCalendar().then(function(result) {
            vm.calendar = result;
            vm.calendarStore[vm.mode] = result;
            ImportWizardService.validateCalendar(vm.calendar).then(function(result) {
                vm.note = (result && result.note ? result.note : '');
            });
        });
    }

    vm.saveCalendar = function() {
        vm.calendar.longerMonth = vm.selectedQuarter;
        ImportWizardService.validateCalendar(vm.calendar).then(function(result) {
            if(!result.errorCode) {
                //console.log('valid calendar, 10/10 would save');
                ImportWizardService.saveCalendar(vm.calendar).then(function(result) {
                    $state.go("home.import.data.product_hierarchy.ids");
                });
            }
        });
    }

    vm.init();
});