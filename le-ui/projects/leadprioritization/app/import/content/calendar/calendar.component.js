angular.module('lp.import.calendar', [])
.controller('ImportWizardCalendar', function(
    $state, $stateParams, $scope, $timeout, $sce, $window,
    NumberUtility, ResourceUtility, ImportWizardStore, ImportWizardService, Calendar, FieldDocument, StateHistory, Modal
) {
    var vm = this,
        debug = false, // goto /import/calendar
        preventUnload = !debug,
        year = new Date().getFullYear(),
        months = ['January','February','March','April','May','June','July','August','September','October','November','December'],
        weekdays = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'],
        picker;

    if(preventUnload) {
        $window.onbeforeunload = function(event) {
            var warning = 'Changes you made may not be saved. Are you sure?';
            event.returnValue = warning;
            return warning;
        };
    }

    $scope.$on("$destroy", function(){
        $window.onbeforeunload = null;
    });

    angular.extend(vm, {
        debug: debug,
        lastFrom: StateHistory.lastFrom(),
        saving: false,
        calendar: Calendar,
        selectedMonth: 1,
        selectedQuarter: '1',
        mode: '',
        calendarStore: {},
        dateParams: {},
        calendarOptions: {}
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
                var dateParams = {
                    monthNumber: date.getMonth() + 1, // starts at 0 for some reason
                    month: months[date.getMonth()].substring(0,3).toUpperCase(), // just use date.getMonth because of array 0 is good place to start
                    day: NumberUtility.PadNumber(date.getDate(),2),
                    year: date.getFullYear()
                }
                vm.dateParams = dateParams;
                selectedDateObj = {
                        mode: "STARTING_DATE",
                        startingDate: dateParams.month + "-" + dateParams.day,
                        evaluationYear: dateParams.year, 
                        longerMonth: vm.selectedQuarter,
                        
                    };
                vm.setCalendar(selectedDateObj);
            }
        });
        field.parentNode.insertBefore(picker.el, field.nextSibling);
        if(vm.calendarOptions && vm.calendarOptions.mode === 'STARTING_DATE') {
            var startingDateMonth = 1;
            months.some(function(month, index) {
                if(vm.calendarOptions.month === month.substring(0,3).toUpperCase()) {
                    startingDateMonth = index + 1;
                }
            });
            vm.dateParams.monthNumber = startingDateMonth;
            vm.dateParams.month = vm.calendarOptions.month;
            vm.dateParams.day = vm.calendarOptions.day;
            vm.selectedQuarter = vm.calendarOptions.longerMonth;

            picker.setDate(('0'+startingDateMonth).slice(-2) + '-' + vm.calendarOptions.day + '-'+year, true); // second param prevents onSelect callback //MM/DD/YYYY
            picker.gotoDate(new Date(year, startingDateMonth - 1));
        } else {
            picker.setDate('01-01-'+year, true); // second param prevents onSelect callback
            picker.gotoDate(new Date(year, 0));
        }
    }

    vm.selectMonth = function(date) {
        vm.selectedMonth = date;
        picker.gotoDate(new Date(year, date - 1));
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

    vm.miniMarkdown = function(string) {
        if(!string) {
            return '';
        }
        var regex = /\*\*(\S(.*?\S)?)\*\*/gm;
            ret = string.replace(regex, '<strong>$1</strong>')
        return $sce.trustAsHtml(ret);
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
                vm.toggleModal();
            }
        });
    }

    vm.initModalWindow = function () {
        vm.modalConfig = {
            'name': "import_calendar",
            'type': 'sm',
            'title': 'Warning',
            'titlelength': 100,
            'dischargetext': 'Cancel',
            'dischargeaction': 'cancel',
            'confirmtext': 'Yes, Update',
            'confirmaction': 'proceed',
            'icon': 'fa fa-exclamation-triangle',
            'iconstyle': {'color': 'white'},
            'confirmcolor': 'blue-button',
            'showclose': true,
            'headerconfig': {'background-color':'#FDC151', 'color':'white'},
            'confirmstyle' : {'background-color':'#FDC151'}
        };

        vm.modalCallback = function (args) {
            if (vm.modalConfig.dischargeaction === args.action) {
                vm.toggleModal();
            } else if (vm.modalConfig.confirmaction === args.action) {
                vm.toggleModal();
            }
            if(args.action === 'proceed') {
                if(debug) {
                    console.log('valid calendar, 10/10 woudl save', vm.lastFrom.name, vm.calendar);
                } else {
                    vm.saving = true;
                    ImportWizardService.saveCalendar(vm.calendar).then(function(result) {
                        $state.go(vm.lastFrom.name);
                    });
                }
            }
        }

        vm.toggleModal = function () {
            var modal = Modal.get(vm.modalConfig.name);
            if (modal) {
                modal.toggle();
            }
        }

        $scope.$on("$destroy", function () {
            Modal.remove(vm.modalConfig.name);
        });
    }

    vm.initModalWindow();

    var parseCalendar = function(calendar) {
        if(!calendar) {
            return false;
        }
        var options = {
                mode: calendar.mode,
                longerMonth: calendar.longerMonth
            };

        if(calendar.mode === 'STARTING_DATE') {
            options.month = calendar.startingDate.split('-')[0];
            options.day = calendar.startingDate.split('-')[1];
        } else if(calendar.mode === 'STARTING_DAY') {
            options.nth = calendar.startingDate.split('-')[0];
            options.day = calendar.startingDate.split('-')[1];
            options.month = calendar.startingDate.split('-')[2];
        }
        vm.calendarOptions = options;
    }

    vm.init = function() {
        //parseCalendar(vm.calendar); // uncomment this to create a non-null state for existing calendars PLS-8479
        if( vm.calendarOptions.mode === 'STARTING_DAY') {
            vm.nthMapping.nth = vm.calendarOptions.nth;
            vm.nthMapping.day = vm.calendarOptions.day;
            vm.nthMapping.month = vm.calendarOptions.month;
            vm.selectedQuarter = vm.calendarOptions.longerMonth;
        }

        if((preventUnload) && !FieldDocument) {
            $state.go('home.import.entry.product_hierarchy');
            return false;
        }

        $timeout(initDatePicker, 0);
    };



    vm.init();
});