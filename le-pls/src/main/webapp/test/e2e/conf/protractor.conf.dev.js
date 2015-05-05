'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "DevelopTestPLSContract.Tenant2.Production";
config.params.alternativeTenantId = "DevelopTestPLSContract.Tenant1.Production";

exports.config = config;