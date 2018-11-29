import React, {
  Component,
  react2angular
} from "../../../../common/react-vendor";
import LeButton from "../../../../common/widgets/buttons/le-button";
import {
  LeToolBar,
  VERTICAL
} from "../../../../common/widgets/toolbar/le-toolbar";
import "./templates.scss";
import { clean, getRouter } from "../react/components/routing-test";
import { UIRouter, UIView } from "../../../../common/react-vendor";
import SummaryContainer from "./components/summary";
import { setAngularState } from "../react/states";
import httpService from "../../../../common/app/http/http-service";
import Observer from "../../../../common/app/http/observer";

class TemplatesComponent extends Component {
  constructor(props) {
    super(props);
    this.emailCredentialConfig = {
      label: "Setup Automation",
      classNames: "gray-button"
    };
    setAngularState(this.props.$state);
  }

  componentDidMount() {
    let router = getRouter();
    router.stateService.go("templatelist");
  }
  componentWillUnmount() {
    clean();
  }

  render() {
    return (
      <div className="le-flex-v-panel main-container center-h">
        <div className="le-flex-v-panel templates-main-container">
          <LeToolBar>
            <div className="right">
              <LeButton
                name="credentials"
                config={this.emailCredentialConfig}
                callback={() => {
                  httpService.get(
                    "/pls/dropbox",
                    new Observer(response => {
                      // console.log("BACK HERE ", response);
                    }),
                    {
                      ErrorDisplayMethod: "Banner",
                      ErrorDisplayOptions: '{"title": "Warning"}',
                      ErrorDisplayCallback: "TemplatesStore.checkIfRegenerate"
                    }
                  );
                }}
              />
            </div>
          </LeToolBar>
          <UIRouter router={getRouter()}>
            <UIView name="reactmain" />
          </UIRouter>
        </div>
      </div>
    );
  }
}

angular
  .module("le.import.templates", [])
  .service("TemplatesStore", function($http, Modal) {
    let TemplatesStore = this;

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
      if(htmlFormat){
        let ret = htmlFormat.replace(/<p>/g, '\r\n');
        ret = ret.replace(/<\/p>/g, '\r\n');
        ret = ret.replace(/<br>/g, '\r\n');
        ret = ret.replace(/<strong>/g, '');
        ret = ret.replace(/<\/strong>/g, '');
      return ret;
      }else{
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
        // let modal = Modal.get(response.name);
        // Modal.modalRemoveFromDOM(modal, { name: response.name });
      }
    };

    this.removeUIActionModal = modalName => {
      let modal = Modal.get(modalName);
      Modal.modalRemoveFromDOM(modal, { name: modalName });
    };
  })
  .component(
    "templatesComponent",
    react2angular(TemplatesComponent, [], ["$state"])
  );
