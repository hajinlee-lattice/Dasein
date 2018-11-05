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
      lable: "Setup Automation",
      classNames: ["button", "gray-button"]
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
        <SummaryContainer />
        <div className="le-flex-v-panel templates-main-container">
          <LeToolBar>
            <div className="right">
              <LeButton
                config={this.emailCredentialConfig}
                callback={() => {
                  httpService.get(
                    "/pls/dropbox",
                    new Observer(response => {
                      // console.log("BACK HERE ", response);
                    }),
                    {
                      ErrorDisplayMethod: "Banner",
                      ErrorDisplayOptions: "{\"title\": \"Warning\"}",
                      ErrorDisplayCallback: "TemplatesStore.regenerate"
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
    this.regenerate = () => {
      $http({
        method: "PUT",
        url: "/pls/dropbox/key",
        headers: {
          ErrorDisplayMethod: "",
          ErrorDisplayOptions: "{\"confirmtext\": \"Download\",\"title\": \"S3 Credentials\"}",
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
        let modal = Modal.get(response.name);
        Modal.modalRemoveFromDOM(modal, { name: response.name });
      }
    };
  })
  .component(
    "templatesComponent",
    react2angular(TemplatesComponent, [], ["$state"])
  );
