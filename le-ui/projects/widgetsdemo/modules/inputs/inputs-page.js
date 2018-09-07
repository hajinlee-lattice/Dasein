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
                placeholder: "Type callback with debounce",
                classNames: ["button", "borderless-button"],
                icon: "fa fa-search",
                clearIcon: true,
                debounce:2000
              }}
              callback={this.callback}
            />
          </div>
          <div>
            <LeInputText
              config={{
                placeholder: "Type call back NOT debounced",
                classNames: ["button", "borderless-button"],
                icon: "",
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
