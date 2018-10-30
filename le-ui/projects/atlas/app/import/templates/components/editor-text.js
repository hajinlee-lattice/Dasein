import React, { Component } from "../../../../../common/react-vendor";
export default class EditorText extends Component {
  constructor(props) {
    super(props);
    this.state = { value: this.props.initialValue, cancelled: false };
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
          console.log(event.keyCode);
          switch(event.keyCode){
            case 27:
            this.setState({cancelled: true}, () => {
              this.nameInput.blur();
            });
            break;
            case 13:
              this.props.saveValue(this.state.value);
            break;
          }
        }}
        onChange={() => {
          this.changeHandler(event);
        }}
        onBlur={() => {
          if(!this.state.cancelled){
            this.props.saveValue(this.state.value);
          }else{
            this.props.cancel();
          }
        }}
      />
    );
  }
}
