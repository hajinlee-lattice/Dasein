import React from "../../react-vendor";
import "./le-v-panel.scss";

const LeVPanel = props => {
  if (props.fillspace) {
    return(
        <div className="le-v-fill">
            <div className="le-v-box">{props.children}</div>
        </div>
    );
  }else{
    return (
        <div className="le-flex-v-panel">{props.children}</div>
    );
  }
};

export default LeVPanel;
