// console.log("Directive module");
// import angular from "angular";

import "./chips.directive";
import "./bar-chart.component";
import './barchart.directive';
import './formOnChange.directive';
import './ngQtipDirective';
import './helperMarkDirective';
import './input-selection.directive';
import './ngEnterDirective';
import './ArcChartDirective';


export default angular.module("com.le.common.directive", [
  "mainApp.appCommon.directives.chips",
  'common.directives.tilebarchart',
  'mainApp.appCommon.directives.barchart',
  'mainApp.appCommon.directives.formOnChange',
  'mainApp.appCommon.directives.ngQtipDirective',
  'mainApp.appCommon.directives.helperMarkDirective',
  'mainApp.appCommon.directives.input.selection',
  'mainApp.appCommon.directives.ngEnterDirective',
  'mainApp.appCommon.directives.charts.ArcChartDirective'
]);
