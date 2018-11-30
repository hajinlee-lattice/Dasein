import React from "../../react-vendor";
import "./le-v-panel.scss";

const LeVPanel = props => {

    return (
        <div className="le-flex-v-panel">{props.children}</div>
    );
};

export default LeVPanel;
