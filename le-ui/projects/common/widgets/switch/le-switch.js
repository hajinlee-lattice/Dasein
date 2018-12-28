import React, { Component } from "common/react-vendor";
import "./le-switch.scss";
class LeSwitch extends Component {
  constructor(props) {
    super(props);
    this.handleChange = this.handleChange.bind(this);
    this.state = {
      isChecked: this.props.isChecked
    };
  }

  render() {
    return (
      <label className="le-switch">
        <input
          onClick={event => {
            this.handleChange(event);
          }}
          type="checkbox"
        />
        <span className="le-slider round" />
      </label>
    );
  }

  handleChange(event) {
    this.setState({ isChecked: event.target.checked }, () => {
      if (this.props.callback) {
        this.props.callback(this.state.isChecked);
      }
    });
  }
}

export default LeSwitch;
