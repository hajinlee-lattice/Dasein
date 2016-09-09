var express = require('express');
var router = express.Router();
var util = require('util')
var baseConfig = require('../public/resources/modelqualitybasic.json');
var request = require("request")

var WebHDFS = require('webhdfs');
var hdfs = WebHDFS.createClient({ user: process.env.USER, host: "10.41.1.185", port: 50070 });

/* GET users listing. */
router.post('/', function(req, res, next) {

 console.log("req.files" + util.inspect(req.body))
 console.log("req.files" + util.inspect(req.body))
 console.log("req.files" + util.inspect(req.files["datasetCSV"]))
 basePath = "/app/a/2.0.38/experiments/"
 var date = new Date()
 randomID = date.getFullYear() + "-" + (date.getMonth() + 1) + "-" +  date.getDate() + "-" + parseInt(Math.random() * 10000000000)
 hdfsDatasetFile= basePath + randomID + "/dataset.csv"
 hdfsPipelineFile= basePath + randomID + "/pipeline.json"
 hdfsPipelineTarFile= basePath + randomID + "/lepipeline.tar.gz"
 
 hdfs.mkdir(basePath + randomID, mode="0777", function(err, success) {
 hdfs.writeFile(hdfsDatasetFile, req.files["datasetCSV"].data, function (err, success) {
        if (err instanceof Error) { console.log(err); }
        console.log(success)
      }); 
hdfs.writeFile(hdfsPipelineFile, req.files["pipelineJSON"].data, function (err, success) {
        if (err instanceof Error) { console.log(err); }
        console.log(success)
      }); 
hdfs.writeFile(hdfsPipelineTarFile, req.files["pipelineTARGZ"].data, function (err, success) {
        if (err instanceof Error) { console.log(err); }
        console.log(success)
      }); 
})
  //Read base-config
  config = baseConfig
  //Update config with path of Hadoop uploaded files
  config["pipeline"]["pipeline_driver"] = hdfsPipelineFile
  config["pipeline"]["pipeline_lib_script"] = hdfsPipelineTarFile
  config["data_set"]["training_hdfs_path"] = hdfsDatasetFile
  // Send post request!
  var url = "https://admin-qa.lattice.local:8080/modelquality/runmodel/?tenant=ModelQualityExperiments.ModelQualityExperiments.Production&username=bnguyen@lattice-engines.com&password=tahoe&apiHostPort=https://bodcdevtca18.lattice.local:8081"
  var tenant="ModelQualityExperiments.ModelQualityExperiments.Production"
  var username="pls-super-admin-tester@test.lattice-engines.com"
  var password="admin"
  var requestData = {
  "name":"NodeModelQualityExperiment",
   "description": "Use node to start model quality job",
  "selectedConfig":config,
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
