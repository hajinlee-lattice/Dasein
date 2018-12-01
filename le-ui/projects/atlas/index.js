/********* Styles ****************************/
import "common/assets/sass/lattice.scss";
/************ This scss files should be removed from this path **************/
import "assets/styles/main.scss";

/**
 * Common
 */

import "common/app/utilities/utilities.index";
import "common/app/services/services.index";
import "common/app/directives/directives.index";
import "components/wizard/wizard.index";
import "components/forms/forms.index";
import "components/components.index";

import "atlas/sfdc/sfsc.index";
import "atlas/config/services/ConfigService";
import "atlas/AppCommon/widgets/widgets.index";
import "atlas/jobs/jobs.index";
import "atlas/create/create.index";
import "atlas/models/models.index";
import "atlas/AppCommon/modules/modules.index";
import "atlas/AppCommon/utilities/utilities.index";
import "atlas/userManagement/user-management.index";

import "components/datacloud/datacloud.index";
import "atlas/navigation/navigation.index";
import "atlas/login/login.index";
import "atlas/ratingsengine/ratingsengine.index";
import "atlas/segments/segments.index";
import "atlas/notes/notes.index";
import "atlas/setup/setup.index";
import "atlas/marketo/marketo.index";
import "atlas/apiConsole/api-console.index";
import "atlas/campaigns/campaigns.index";
import "atlas/playbook/playbook.index";
import "atlas/cgtalkingpoint/cg-talking-point.index";
import "atlas/import/import.index";
import "atlas/delete/delete.index";
import "atlas/configureattributes/configure-attributes.index";
import "atlas/ssoconfig/ssocinfig.index";
import "./templates-main";
import "atlas/app";
import "atlas/routes";
import "./help.index";
import "./zendesk.index";

export default angular.module("com.le.atlas", [
    "com.le.common.services",
    "templates-main",
    "mainApp"
]);
