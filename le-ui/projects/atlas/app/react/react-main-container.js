import React from "common/react-vendor";
import "./react-main.component.scss";

const ReactMainContainer = props => {
	return (
		<div className={`${"main-content"} ${props.className}`}>
			{props.children}
		</div>
	);
};

export default ReactMainContainer;
