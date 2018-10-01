import React, { Component } from "../../react-vendor";
import debounce from "../utilities/debounce";

import "./le-input-text-float.scss";

class LeInputTextFloat extends Component {
  constructor(props) {
    super(props);
    
  }

  
  render() {
    return (
      <div className="input-float">
        
        <input type="text" />

      </div>
    );
  }
}

export default LeInputTextFloat;
