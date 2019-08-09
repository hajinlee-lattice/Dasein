import React, { Component } from "common/react-vendor";

export default class IFrameComponent extends Component {
	constructor(props) {
		super(props);
		this.myRef = React.createRef();
	}

	componentDidMount() {
		// this.refs.iframe
		// 	.getDOMNode()
		// 	.addEventListener("load", this.props.onLoad);
		console.log(this.props);
		this.props.mounted(this.myRef);
		// this.props.mounted(this.myRef);
	}
	render() {
		return (
			<iframe
				ref={this.myRef}
				{...this.props}
				name={this.props.name}
				id={this.props.name}
			/>
		);
	}
}
