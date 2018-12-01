import React, { Component, UIView } from "common/react-vendor";
import { getAngularState } from "../states";
import { getRouter } from "./routing-test";
import LeButton from "widgets/buttons/le-button";
import { LeToolBar, VERTICAL } from "widgets/toolbar/le-toolbar";
import "./about.scss";

class About extends Component {
    constructor(props) {
        super(props);
        console.log("PROPS ", props);
        this.myDataButtonConfig = {
            lable: "My Data",
            classNames: ["button", "orange-button"],
            image: "fa fa-arrow-left"
        };

        this.nextButtonConfig = {
            lable: "Email Automation Credentials",
            classNames: ["button", "gray-button"]
        };
    }

    render() {
        return (
            <div className="le-flex-v-panel test">
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
                                getRouter().stateService.go("hello");
                            }}
                        />
                    </div>
                </LeToolBar>
                <UIView name="reactsub" />
            </div>
        );
    }
}
export default About;
