// console.log("Wizard module");
// import angular from "angular";

import '../../app/utilities/ResourceUtility';
// import '../../app/modules/ServiceError/ServiceErrorModule';
import './progress/progress.component';

import './controls/controls.component';

import './header/header.component';

import './wizard.component';


export default angular.module("com.le.common.wizard", [
    'common.wizard'
]);
