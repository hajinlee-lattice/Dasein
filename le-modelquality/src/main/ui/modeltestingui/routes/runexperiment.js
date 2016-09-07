var express = require('express');
var router = express.Router();
var config = require('../public/resources/modelqualitybasic.json');
var request = require("request")

/* GET users listing. */
router.get('/', function(req, res, next) {
  str = config
  //obj = JSON.parse(config)
 console.log(str.tenant)
 console.log(str)

 
  // Send post request!
  var url = "https://admin-qa.lattice.local:8080/modelquality/runmodel/?tenant=ModelQualityExperiments.ModelQualityExperiments.Production&username=bnguyen@lattice-engines.com&password=tahoe&apiHostPort=https://bodcdevtca18.lattice.local:8081"
  var tenant="ModelQualityExperiments.ModelQualityExperiments.Production"
  var username="pls-super-admin-tester@test.lattice-engines.com"
  var password="admin"
  var requestData = {
  "name":"NodeModelQualityExperiment",
   "description": "Use node to start model quality job",
  "selectedConfig":str,
  "status": "NEW"
  }

  request({
        url: url,
        method: "POST",
        json: true,
        headers: {
                    'content-type': 'application/json'
        },
        body: requestData,
	rejectUnauthorized: false
    }, function (error, response, body) {
        if (!error && response.statusCode === 200) {
            console.log(body)
	    res.send('respond with a resource' + JSON.stringify(body));
        }
        else {

            console.log("error: " + error)
            console.log("response: " + JSON.stringify(response))
            console.log("response.statusCode: " + response.statusCode)
            console.log("response.statusText: " + response.statusText)
           res.send('respond with a resource' + JSON.stringify(response.body));
        }
    })

});

module.exports = router;
