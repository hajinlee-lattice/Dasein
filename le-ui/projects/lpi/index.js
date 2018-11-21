/********* Styles ****************************/
import "../common/assets/sass/lattice.scss";
/************ This scss files should be removed from this path **************/
import './assets/styles/main.scss';

/**
 * Common 
 */

import '../common/app/utilities/utilities.index';
import '../common/app/services/services.index';
import '../common/app/directives/directives.index';
import '../common/components/wizard/wizard.index';
import '../common/components/forms/forms.index';
import '../common/components/components.index';

import './app/sfdc/sfsc.index';
import './app/config/services/ConfigService';
import './app/AppCommon/widgets/widgets.index';
import './app/jobs/jobs.index';
import './app/create/create.index';
import './app/models/models.index';
import './app/AppCommon/modules/modules.index';
import './app/AppCommon/utilities/utilities.index';
import './app/userManagement/user-management.index';

import '../common/components/datacloud/datacloud.index';
import './app/navigation/navigation.index';
import './app/login/login.index';
// import './app/ratingsengine/ratingsengine.index';
// import './app/segments/segments.index';
import './app/notes/notes.index';
import './app/setup/setup.index';
import './app/marketo/marketo.index';
import './app/apiConsole/api-console.index';
// import './app/campaigns/campaigns.index';
// import './app/playbook/playbook.index';
import './app/cgtalkingpoint/cg-talking-point.index';
import './app/import/import.index';
import './app/delete/delete.index';
import './app/configureattributes/configure-attributes.index';
import './app/ssoconfig/ssocinfig.index';
import "./templates-main";
import './app/app';
import './app/routes';
import './help.index';
import './zendesk.index';

export default angular.module("com.le.lpi", ['com.le.common.services',"templates-main", 
"mainApp"]);