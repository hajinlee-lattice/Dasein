import './assets/css/font-awesome.min.css';
import WebFont from 'webfontloader';

/** CryptoJS used in the login app */
window["CryptoJS"] = require("crypto-js");

window._ = require('underscore');
export const underscore = require('underscore');

window["dateFormat"] = require('dateformat');

/*************** Date picker ******************/
window["Pikaday"] = require('./lib/bower/pikaday.js');
import './assets/css/pickaday.css'
/*********************************************/
require('bootstrap3');
require('./lib/js/ui-bootstrap.js');

require('./lib/bower/min/ocLazyLoad.min');

export const moment = require('./lib/bower/moment.js');

WebFont.load({
  google: {
    families: ['Droid Sans', 'Droid Serif']
  }
});