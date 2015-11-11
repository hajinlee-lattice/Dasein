describe('user management', function() {

    var externalUser = require('./po/externaluser.po');
    var externalAdmin = require('./po/externaladmin.po');
    var internalUser = require('./po/internaluser.po');
    var internalAdmin = require('./po/internaladmin.po');
    var superAdmin = require('./po/superadmin.po');

    //=======================================================
    // All user management tests are distributed to each role
    //=======================================================
    externalUser.testUserManagement();
    externalAdmin.testUserManagement();
    internalUser.testUserManagement();
    internalAdmin.testUserManagement();
    superAdmin.testUserManagement();
});

