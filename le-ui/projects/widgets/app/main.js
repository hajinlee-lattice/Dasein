angular.module('leWidgets', [
    'ui.router',
    'dd.test',
    'barchart.test',
    'modal.test',
    'datepicker.test'
  ])
  .config(function ($stateProvider, $locationProvider) {
    $locationProvider.html5Mode(true);
    var states = [{
        name: 'barchart',
        url: '/barchart',
        component: 'barChartTest'
      },
      {
        name: 'date',
        url: '/date',
        component: 'datePickerTest'
      },
      {
        name: 'dropdown',
        url: '/drop',
        component: 'dropDownTest',
        resolve: {
          input: function(){
            return [
              {value: 1, name: 'Option 1'},
              {value: 2, name: 'Option 2'},
              {value: 3, name: 'Option 3'},
              {value: 4, name: 'Option 4'}
            ];
          },
          selected: function(){
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