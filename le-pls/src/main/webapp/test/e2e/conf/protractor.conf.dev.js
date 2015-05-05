'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantName = "QATest Tenant 1";
config.params.alternativeTenantName = "QATest Tenant 2";

exports.config = config;