'use strict';

var Login = function() {
	this.email = element(by.model('username'));
	this.password = element(by.model('password'));
    this.loginButton = element(by.id('loginButton'));
 
  this.get = function() {
    browser.get('/');
  };

};

module.exports = new Login();