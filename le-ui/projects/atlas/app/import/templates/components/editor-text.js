import React, { Component } from "../../../../../common/react-vendor";
export default class EditorText extends Component {
  constructor(props) {
    super(props);
    console.log(this.props.initialValue);
    this.state = { value: this.props.initialValue };
  }

  componentDidMount() {
    this.nameInput.focus();
  }
  changeHandler(event) {
    this.setState({ value: event.target.value });
    // console.log(event.target.value);
    // this.value = event.target.value;
  }
  render() {
    return (
      <input
        type="text"
        ref={(input) => { this.nameInput = input; }} 
        value={this.state.value}
        onChange={() => {
          this.changeHandler(event);
        }}
        onBlur={() => {
          this.props.saveValue(this.state.value);
        }}
      />
    );
  }
}
