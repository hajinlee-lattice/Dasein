angular.module('lp.import.calendar', [])
.controller('ImportWizardCalendar', function(
    $state, $stateParams, $scope, $timeout, ResourceUtility, ImportWizardStore, Calendar, FieldDocument
) {
    var vm = this,
        preventUnload = false,
        year = new Date().getFullYear(),
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
        selectedQuarter: '1st'
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
                months: ['January','February','March','April','May','June','July','August','September','October','November','December'],
                weekdays: ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'],
                weekdaysShort : ['S','M','T','W','T','F','S']
            },
            onSelect: function(date) {
                selectedDateObj = {
                        day: date.getDate(),
                        month: date.getMonth() + 1,
                        year: date.getFullYear()
                    };
                console.log(selectedDateObj);
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
    }

    vm.changeNth = function(nthMapping) {
        console.log(nthMapping);
    }

    vm.setCalendar = function(obj) {
        ImportWizardStore.setCalendar(obj);
        ImportWizardStore.getCalendar().then(function(result) {
            vm.calendar = result;
        });
    }

    vm.init();
});