import React, { Component } from "../../../common/react-vendor";
import Aux from "../../../common/widgets/hoc/_Aux";
import "./inputs-page.scss";
import LeInputText from "../../../common/widgets/inputs/le-input-text";

import "../../../common/widgets/layout/layout.scss";

export default class InputsPage extends Component {
  constructor(props) {
    super(props);
    console.log("Component initialized");
  }

  callback(val) {
   console.log('Callback CALLED ', val);
  }

  render() {
    
    return (
      <Aux>
        <form className="border-container le-flex-v-panel">
          <div>
            <LeInputText
              config={{
                placeholder: "Callback with debounce 2s",
                classNames: ["button", "borderless-button"],
                icon: "fa fa-search",
                label: "First Name",
                clearIcon: true,
                debounce:2000
              }}
              callback={this.callback}
            />
          </div>
          <div>
            <LeInputText
              config={{
                placeholder: "Callback NOT debounced",
                classNames: ["button", "borderless-button"],
                icon: "",
                label: "Address with number and zip code",
                clearIcon: true
              }}
              callback={this.callback}
            />
          </div>
          <div>
            <LeInputText
              config={{
                placeholder: "Callback NOT debounced",
                classNames: ["button", "borderless-button"],
                clearIcon: false
              }}
              callback={this.callback}
            />
          </div>
        </form>
      </Aux>
    );
  }
}
