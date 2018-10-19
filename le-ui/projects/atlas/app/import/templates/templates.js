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
import SummaryContainer from "./summary";
import { setAngularState } from "../react/states";

class TemplatesComponent extends Component {
  constructor(props) {
    super(props);
    this.nextButtonConfig = {
      lable: "Email Automation Credentials",
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
            {/* <div className="left">
            <LeButton
              config={this.myDataButtonConfig}
              callback={() => {
                getAngularState().go("home");
              }}
            />
          </div> */}
            <div className="right">
              <LeButton
                config={this.nextButtonConfig}
                callback={() => {
                  // getRouter().stateService.go("hello");
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
  .component(
    "templatesComponent",
    react2angular(TemplatesComponent, [], ["$state"])
  );
