import React, { Component } from "common/react-vendor";
import "./le-chip.scss";
import search from "./le-search.utils";

class LeChip extends Component {
  constructor(props) {
    super(props);
    this.removeChip = this.removeChip.bind(this);
    this.state = {
      value: "",
      chipsSelected: [],
      listElements: [],
      idSelected: {},
      show: false,
      selected: 0
    };
  }

  removeChip(item, position) {
    let tmpChips = [...this.state.chipsSelected];
    tmpChips.splice(position, 1);
    const idsCopy = { ...this.state.idSelected };
    delete idsCopy[item.id];
    this.setState({ chipsSelected: tmpChips, idSelected: idsCopy });
  }

  getListItems() {
    let listItems = this.state.listElements.map((item, index) => {
      if (!this.state.idSelected[item.id]) {
        return (
          <li
            key={index}
            className={`${"le-chips-list-item"} ${
              this.state.selected == index ? "selected" : ""
            }`}
            onClick={event => {
              console.log("Clicked");
              this.addChip(item);
            }}
          >
            {item.displayName}
          </li>
        );
      }
    });

    return listItems;
  }
  keyPressedHandler(event) {
    event.stopPropagation();
    // console.log(event.keyCode);
    switch (event.keyCode) {
      case 40:
      //ArrowDown
        if (this.state.selected + 1 < this.state.listElements.length) {
          this.setState({ selected: this.state.selected + 1 });
        }
        break;
      case 38:
      //ArrowUp
        if (this.state.selected - 1 >= 0) {
          this.setState({ selected: this.state.selected - 1 });
        }
        break;
      case 27:
      //Escape
        this.setState({ show: false });
      break;

      case 8:
      //Backspace
      if(this.state.value == ''){
        this.setState({ show: false });
      }
      case 32:
      //Space with ctrl
      if(event.ctrlKey){
        this.setState({ show: true });
      }
      break;
    }
    
  }
  searchItems(event) {
    let val = event.target.value;
    let itemsFiltered = search(this.props.listItems, val, "displayName");
    this.setState({ value: val, listElements: itemsFiltered, show: true });
  }

  addChip(item) {
    let tmpChips = [...this.state.chipsSelected];
    tmpChips.push(item);
    this.setState({ chipsSelected: tmpChips });
    const idsCopy = { ...this.state.idSelected };
    idsCopy[item.id] = item.name;
    this.setState({ idSelected: idsCopy });
    // console.log(this.state.chipsSelected);
  }

  getChipsSelected() {
    let chipsUI = this.state.chipsSelected.map((item, index) => {
      return (
        <li key={index} className="le-chip">
          <span className="le-chip-name">{item.displayName}</span>
          <i
            className="le-chip-remove fa fa-times"
            aria-hidden="true"
            onClick={() => {
              this.removeChip(item, index);
            }}
          />
        </li>
      );
    });
    return chipsUI;
  }

  render() {
    console.log("Rendering ", this.state);
    return (
      <div className="le-chips">
        <ul className="le-chips-selected">{this.getChipsSelected()}</ul>
        <div className="le-chip-input">
          <i className="fa fa-search" aria-hidden="true" />
          <input
            type="text"
            ref={(input) => { this.chipsInput = input; }} 
            value={this.state.value}
            onKeyUp={event => {
              this.keyPressedHandler(event);
            }}
            onChange={event => {
              this.searchItems(event);
            }}
            onBlur={event => {
              setTimeout(() => {
                this.setState({ show: false });
              }, 200);
            }}
          />
          <div
            className={`${"le-chip-dropdown"} ${
              this.state.show != "" ? "le-chip-dropdown-show" : ""
            }`}
          >
            <ul
              className={`${"le-chip-dropdown-content"} ${
                this.state.show != "" ? "le-chip-dropdown-show" : ""
              }`}
            >
              {this.getListItems()}
            </ul>
          </div>
        </div>
      </div>
    );
  }
}

export default LeChip;
