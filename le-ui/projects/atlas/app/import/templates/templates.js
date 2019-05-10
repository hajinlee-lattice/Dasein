angular
  .module("le.import.templates", [])
  .service("TemplatesStore", function ($http, Modal) {
    let TemplatesStore = this;

    this.newToken = () => {
      $http({
        method: "GET",
        url: "/pls/dropbox",
        headers: {
          ErrorDisplayMethod: "Banner",
          ErrorDisplayOptions: '{"title": "Warning"}',
          ErrorDisplayCallback: "TemplatesStore.checkIfRegenerate"
        },
        data: { AccessMode: "LatticeUser" }
      })
    };

    this.checkIfRegenerate = data => {
      switch (data.action) {
        case "ok":
          TemplatesStore.regenerate();
          break;

        case "cancel":
          TemplatesStore.removeUIActionModal(data.name);
          break;
      }
    };
    this.txtFormat = (htmlFormat) => {
      if (htmlFormat) {
        let ret = htmlFormat.replace(/<p>/g, '\r\n');
        ret = ret.replace(/<\/p>/g, '\r\n');
        ret = ret.replace(/<br>/g, '\r\n');
        ret = ret.replace(/<strong>/g, '');
        ret = ret.replace(/<\/strong>/g, '');
        return ret;
      } else {
        return 'Please contact your Admin';
      }
    };
    this.regenerate = () => {
      $http({
        method: "PUT",
        url: "/pls/dropbox/key",
        headers: {
          ErrorDisplayMethod: "",
          ErrorDisplayOptions:
            '{"confirmtext": "Download","title": "S3 Credentials"}',
          ErrorDisplayCallback: "TemplatesStore.download"
        },
        data: { AccessMode: "LatticeUser" }
      }).then(
        function onSuccess(response) {
          Modal.data = response.data.UIAction.message;
        },
        function onError(response) {
          console.log(response);
        }
      );
    };

    this.getExistingToken = () => {
      $http({
        method: "GET",
        url: "/pls/dropbox/key",
        headers: {
          ErrorDisplayMethod: "",
          ErrorDisplayOptions:
            '{"confirmtext": "Download","title": "S3 Credentials"}',
          ErrorDisplayCallback: "TemplatesStore.download"
        },
        data: { AccessMode: "LatticeUser" }
      }).then(
        function onSuccess(response) {
          Modal.data = response.data.UIAction.message;
        },
        function onError(response) {
          console.log(response);
        }
      );
    };


    this.download = response => {
      if (response && response.action != "closedForced") {
        let toDownload = Modal.data;
        toDownload = TemplatesStore.txtFormat(toDownload);
        var element = document.createElement("a");
        element.setAttribute(
          "href",
          "data:text/plain;charset=utf-8," + encodeURIComponent(toDownload)
        );
        element.setAttribute("download", "atlas_credentials.txt");
        element.style.display = "none";
        document.body.appendChild(element);
        element.click();
        document.body.removeChild(element);
        TemplatesStore.removeUIActionModal(response.name);
      }
    };

    this.removeUIActionModal = modalName => {
      let modal = Modal.get(modalName);
      Modal.modalRemoveFromDOM(modal, { name: modalName });
    };
  });