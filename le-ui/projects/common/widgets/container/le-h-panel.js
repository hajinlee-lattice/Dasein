import React from "../../react-vendor";
import "./le-h-panel.scss";

const LeHPanel = props => {
  let pushRight = props.rightAllign ?  'push-right' : '';
  if (props.fillspace) {
    return(
        <div className={`le-h-fill ${props.classes ? props.classes : ''}`}>
            <div className={`le-h-box ${pushRight} ${props.classes ? props.classes : ''}`}>{props.children}</div>
        </div>
    );
  }else{
    return (
        <div className={`le-flex-h-panel ${pushRight} ${props.classes ? props.classes : ''}`}>{props.children}</div>
    );
  }
};

export default LeHPanel;
