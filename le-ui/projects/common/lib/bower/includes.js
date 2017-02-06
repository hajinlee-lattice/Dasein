 /**
  * polyfill for ie
  * @param  {[type]} !String.prototype.includes [description]
  * @return {[type]}                            [description]
  */
 if (!String.prototype.includes) {
     String.prototype.includes = function() {
         'use strict';
         return String.prototype.indexof.apply(this, arguments) !== -1;
     };
 }