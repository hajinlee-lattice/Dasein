import React, { Component } from "../../../../../common/react-vendor";
export default class EditorText extends Component {
  constructor(props) {
    super(props);
    console.log(this.props.initialValue);
    this.resetValue = this.props.initialValue;
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
        onKeyDown={(event) => {
          if(27 == event.keyCode){
            this.setState({value: this.resetValue})
            this.nameInput.value = this.props.initialValue; 
            this.nameInput.blur();
          }
          console.log(event.keyCode);
        }}
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
