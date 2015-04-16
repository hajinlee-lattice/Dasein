'use strict';

var Logout = function() {
    var userDropdown = require('./userdropdown.po');
 
    this.logout = function(name) {
        userDropdown.toggleDropdown().click();
        browser.driver.wait(function(){
            return userDropdown.signout.isPresent();
        }, 10000, 'dropdown menu should appear with in 10 sec.');
        userDropdown.signout.click();
        browser.driver.sleep(3000);
    };

  	this.logoutAsAdmin = function() {
        this.logout(browser.params.adminDisplayName);
    };

  	this.logoutAsNonAdmin = function() {
        this.logout(browser.params.nonAdminDisplayName);
    };

};

module.exports = new Logout();