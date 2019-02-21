angular.module('lp.import.calendar', [])
.controller('ImportWizardCalendar', function(
    $state, $stateParams, $scope, $timeout, $sce, $window,
    NumberUtility, ResourceUtility, ImportWizardStore, ImportWizardService, Calendar, DateRange, FieldDocument, StateHistory, Modal, Banner
) {
    var vm = this,
        debug = false, // goto /import/calendar
        preventUnload = false, //!debug,
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
        var regex = /\*\*(\S(.*?\S)?)\*\*/gm,
            ret = string.replace(regex, '<strong>$1</strong>');
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
                Modal.warning({
                    name: 'calendar_warning',
                    title: "Business Calendar",
                    message: 'The <strong>Custom</strong> Calendar will be updated throughout the product, including imported data.',
                    confirmtext: "Yes, Update"
                }, vm.modalCallback);
            }
        });
    }

    vm.saveStandardCalendar = function() {
        Modal.warning({
            name: 'standard_calendar_warning',
            title: "Standard Calendar",
            message: 'The <strong>Standard</strong> Calendar will be updated throughout the tenant, including imported data.',
            confirmtext: "Yes, Update"
        }, vm.modalCallbackStandardCalendar);
    }

    var parseCalendar = function(calendar) {
        if(!calendar) {
            return false;
        }
        var options = {
                mode: calendar.mode,
                longerMonth: calendar.longerMonth
            };

        if(calendar.mode === 'STARTING_DATE') {
            var startingDateAr = calendar.startingDate.split('-');
            options.month = startingDateAr[0];
            options.day = startingDateAr[1];
        } else if(calendar.mode === 'STARTING_DAY') {
            var startingDayAr = calendar.startingDay.split('-');
            options.nth = startingDayAr[0];
            options.day = startingDayAr[1];
            options.month = startingDayAr[2];
        }
        vm.calendarOptions = options;
        vm.changeMode(vm.calendarOptions.mode);
    }

    vm.useStandardCalendar = function() {
        vm.saveStandardCalendar();
        /**
         * If you wanted to change the calendar you'd do as below
         */
        
        // var mode = 'STARTING_DATE';
        // vm.mode = mode;
        // var date = new Date(),
        //     _day = '03',
        //     _month = '11',
        //     year = date.getFullYear(),
        //     day = parseInt(_day),
        //     dayText = _day,
        //     month = parseInt(_month),
        //     monthText = months[month - 1].substring(0,3).toUpperCase(),
        //     standardCalendarObj = {
        //         mode: mode,
        //         startingDate: monthText + '-' + dayText,
        //         evaluationYear: year, 
        //         longerMonth: '1',
        //     };
        // console.log(standardCalendarObj);
        // picker.setDate(monthText + '-' + dayText + '-' + year); // MM/DD/YYYY - second param prevents onSelect callback
        // picker.gotoDate(new Date(year, month - 1));  
    }

    vm.modalCallback = function (args) {
        var modal = Modal.get('calendar_warning');
        if ('cancel' === args.action) {
            Modal.modalRemoveFromDOM(modal, {name: 'calendar_warning'});
            // vm.toggleModal();
        } else if ('ok' === args.action) {
            modal.waiting(true);
            if(debug) {
                console.log('valid calendar, 10/10 woudl save', vm.lastFrom.name, vm.calendar);
            } else {
                vm.saving = true;
                ImportWizardStore.saveCalendar(vm.calendar).then(function(result) {
                    Modal.modalRemoveFromDOM(modal, {name: 'calendar_warning'});
                    $state.go('home');
                });
            }
        }
    }

    vm.modalCallbackStandardCalendar = function (args) {
        var modal = Modal.get('standard_calendar_warning');
        if ('cancel' === args.action) {
            Modal.modalRemoveFromDOM(modal, {name: 'standard_calendar_warning'});
            // vm.toggleModal();
        } else if ('ok' === args.action) {
            modal.waiting(true);
            if(debug) {
                console.log('valid calendar, 10/10 woudl save', vm.lastFrom.name, vm.calendar);
            } else {
                vm.saving = true;
                Modal.modalRemoveFromDOM(modal, {name: 'standard_calendar_warning'});

                var deleteCalender = false;
                if(deleteCalender) {
                    // this just deletes the calendar, then standard calendar is used by default
                    ImportWizardService.deleteCalendar().then(function(result) {
                        vm.saving = false;
                        $state.go('home');
                    });
                } else {
                    //this sets a blank calendar in standard mode (new way to set it rather then delete)
                    ImportWizardStore.saveCalendar({
                        mode: 'STANDARD',
                        startingDate: '',
                        evaluationYear: year,
                        longerMonth: '1'
                    }).then(function(result) {
                        vm.saving = false;
                        $state.go('home');
                    });
                }
            }
        }
    }

    vm.init = function() {
        var calendarInfo = ImportWizardStore.getCalendarInfo(Calendar),
            start_date = DateRange[0],
            end_date = DateRange[1],
            bannerMessage = '';

        if(calendarInfo.mode !== 'NONE') {
            bannerMessage = `This tenant is currently using the <strong> ${calendarInfo.modeDisplayName} Mode</strong>. The year starts on <strong>${start_date}</strong> and ends on <strong>${end_date}</strong>.`;
        } else {
            bannerMessage = `This tenant does not have a calendar configured.`;
        }

        Banner.info({
            message: bannerMessage
        });

        parseCalendar(vm.calendar);
        if( vm.calendarOptions.mode === 'STARTING_DAY') {
            vm.nthMapping = vm.nthMapping || {};
            vm.nthMapping.nth = vm.calendarOptions.nth;
            vm.nthMapping.day = vm.calendarOptions.day;
            vm.nthMapping.month = vm.calendarOptions.month;
            vm.selectedQuarter = vm.calendarOptions.longerMonth;
        }

        $timeout(initDatePicker, 0);
    };

    vm.init();
});