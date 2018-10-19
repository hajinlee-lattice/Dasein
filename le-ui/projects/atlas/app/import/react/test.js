import React, {
  Component,
  react2angular
} from "../../../../common/react-vendor";
import LeButton from "../../../../common/widgets/buttons/le-button";
import "../../../../common/widgets/layout/layout.scss";
import { clean, getRouter } from "./components/routing-test";
import { UIRouter, UIView } from "../../../../common/react-vendor";
import Navigation from "./components/Nav";

class TestComponent extends Component {
  constructor(props) {
    super(props);
    this.myDataButtonConfig = {
      lable: "My Data",
      classNames: ["button", "orange-button"],
      image: 'ico ico-analysis'
    };
  }
 
  componentWillUnmount() {
    clean();
  }

  render() {
    return (
      <div id="templatesimp" className="le-flex-v-panel">
        <div className="le-flex-h-panel">
          <LeButton
            config={this.myDataButtonConfig}
            callback={() => {
              this.props.$state.go("home");
            }}
          />
        </div>
        <div className="le-flex-v-panel">
          <UIRouter router={getRouter()}>
            <Navigation />
            <UIView name="reactmain"/>
          </UIRouter>
        </div>
      </div>
    );
  }
}

angular
  .module("le.import.testleo", [])
  .component("testComponent", react2angular(TestComponent, ["id"], ["$state"]));
