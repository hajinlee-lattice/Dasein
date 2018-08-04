console.log("Entry point of insight app");

import "../common/assets/sass/lattice.scss";

import '../common/app/modules/modules.index';
import '../common/app/directives/directives.index';
import '../common/app/utilities/utilities.index';
import '../common/components/datacloud/datacloud.index';

import '../common/app/services/services.index';



import '../common/components/components.index';

import './templates-main';
import './app/app.component';
import './app/app.routes';

export default angular.module("com.le.insight", ["templates-main","insightsApp"]);
