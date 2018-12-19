/** JQuery */
var $ = require("jquery");
window["$"] = $;
window["jQuery"] = $;
window["window.jQuery"] = $;

/** Angular js library */
require("angular");
/** UI-router library */
require("@uirouter/angularjs");
/** redux */
//require("./app/store");

/** ngRoute -- */
require("./lib/js/angular/angular-route.js");

require("./lib/bower/min/angular-tooltips.min.js");

require("./lib/bower/min/angular-sanitize.min.js");

require("./lib/bower/momentangular-moment.js");
require("angulartics");
require("angulartics-mixpanel");
require("./lib/bower/min/angular-animate.min.js");
