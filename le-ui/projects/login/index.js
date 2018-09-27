console.log("Entry point of login app");
// console.log('======================');

import "../common/assets/sass/lattice.scss";
import "./app/app.component.scss";

import "./templates-main";

import "../common/lib/bower/ui-bootstrap-jpls-0.13.0";

import "../common/app/utilities/utilities.index";
import "../common/app/services/services.index";
// import "../common/app/modules/modules.index";
import '../common/components/components.index';
import "../common/app/directives/ngEnterDirective";

import "./app/login/login.routes";
import "./app/login/login.component";

import "./app/login/login.service";
import "./app/login/form/form.component";

import "./app/login/update/update.component";

import "./app/login/forgot/forgot.component";

import "./app/login/tenants/tenants.component";

import "./app/login/saml/saml.component";
import "./app/login/saml/logout/logout.component";

import "./app/login/saml/error/error.component";
import "./app/login/logout/logout.component";

import '../common/components/banner/banner.component';
import '../common/components/modal/modal.component';
import '../common/components/notice/notice.component';
import '../common/components/exceptions/exceptions.component';
import '../common/components/pagination/pagination.component';


import "./app/app.component";
import "./app/app.routes";

import '../atlas/help.index';

export default angular.module("com.le.login", ["templates-main", "loginApp"]);
