import "./styles/dev-styles.scss";
import "./components/test";
import "./tests/drop-down.test";
import "./tests/bar-chart/bar-chart.test";
import "./tests/chip/chip.test";
import "./tests/date/date-picker.test";
import "./tests/modal/modal.test";
import "./tests/numerical/numerical.test";

// import style from 'main.scss';
angular.module('leWidgets', [
        'ui.router',
        'leWidgets.test',
        'barchart.test',
        'dd.test',
        'chip.test',
        'datepicker.test',
        'modal.test',
        'numerical.test'
    ])
    .config(function ($stateProvider, $locationProvider) {
        $locationProvider.html5Mode(true);

        var states = [
            {
                name: 'barchart',
                url: '/barchart',
                component: 'barChartTest'
              },
              {
                name: 'test',
                url: '/test',
                component: 'test'
            },
            {
                name: 'daterange',
                url: '/daterange',
                component: 'datePickerTest'
            },
            {
                name: 'numrange',
                url: '/numrange',
                component: 'numericalTest'
            },
            {
                name: 'chip',
                url: '/chip',
                component: 'chipTest'
            },
            {
                name: 'dropdown',
                url: '/dropdown',
                component: 'dropDownTest',
                resolve: {
                    input: function () {
                        return [{
                                value: 1,
                                name: 'Option 1'
                            },
                            {
                                value: 2,
                                name: 'Option 2'
                            },
                            {
                                value: 3,
                                name: 'Option 3'
                            },
                            {
                                value: 4,
                                name: 'Option 4'
                            }
                        ];
                    },
                    selected: function () {
                        return 1;
                    }
                }
            },
            {
                name: 'modal',
                url: '/modal',
                component: 'modalTest'
            }
        ];

        states.forEach(function (state) {
            $stateProvider.state(state);
        });
    });

//   .component('test', {
//     template: txt,

//     bindings: {
//         selection: '<',
//         options: '=',
//         callback: '&'
//     },

//     controller: function () {

//         function getOptions() {
//             return this.options;
//         }

//         function valueChanged(val) {
//             if (this.callback !== undefined) {
//                 var selection = JSON.stringify(this.selected);
//                 this.callback({
//                     value: val
//                 });
//             }
//         }
//         this.$onInit = function () {
//             if (!this.options || this.options == null) {
//                 this.options = [];
//             }
//             this.options.unshift({
//                 value: -1,
//                 name: '-- Select Option --'
//             });
//             if (!this.selection) {
//                 this.selection = this.options[0];
//             }

//             console.log('');

//         };

//         this.changed = valueChanged;
//         this.getOptions = getOptions;
//     }

// });