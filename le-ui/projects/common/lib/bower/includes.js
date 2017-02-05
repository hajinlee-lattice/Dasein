 /**
  * polyfill for ie
  * @param  {[type]} !String.prototype.includes [description]
  * @return {[type]}                            [description]
  */
 console.log(1111);
 if (!string.prototype.includes) {
     string.prototype.includes = function() {
         'use strict';
         return string.prototype.indexof.apply(this, arguments) !== -1;
     };
 }