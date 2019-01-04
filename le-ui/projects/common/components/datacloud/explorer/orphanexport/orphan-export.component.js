angular.module("lp.jobs.orphan.export", []).component("orphanExport", {
  templateUrl:
    "/components/datacloud/explorer/orphanexport/orphan-export.component.html",
  bindings: {},
  controller: function($stateParams, SegmentService) {
    this.$onInit = function() {
      this.exportID = $stateParams.exportID;
      this.downloadOrphanExport();
    };
   
    this.downloadOrphanExport = function() {
      if (this.exportID && this.exportID !== null) {
        SegmentService.DownloadExportedOrphans(this.exportID).then(result => {
          var contentDisposition = result.headers("Content-Disposition");
          var element = document.createElement("a");
          var fileName = contentDisposition.match(/filename="(.+)"/)[1];
          element.download = fileName;
          var file = new Blob([result.data], {
            type: "application/octect-stream"
          });
          var fileURL = window.URL.createObjectURL(file);
          element.href = fileURL;
          document.body.appendChild(element);
          element.click();
          document.body.removeChild(element);
        });
      }
    };
  }
});
