'use strict';

var Logout = function() {
    var loginPage = require('./login.po');
    var userDropdown = require('./userdropdown.po');    
 
    this.logout = function(name) {
        userDropdown.getUserLink(name).click();
        browser.driver.wait(function(){
            return userDropdown.signout.isPresent();
        }, 10000, 'dropdown menu should appear with in 10 sec.');
        userDropdown.signout.click();
        browser.driver.sleep(3000);
        //browser.driver.wait(function(){
        //    return element(by.model('username')).isPresent();
        //}, 20000, 'login page should appear with in 20 sec.');
    };

  	this.logoutAsAdmin = function() {
        this.logout(browser.params.adminDisplayName);
    };

  	this.logoutAsNonAdmin = function() {
        this.logout(browser.params.nonAdminDisplayName);
    };

};

module.exports = new Logout();