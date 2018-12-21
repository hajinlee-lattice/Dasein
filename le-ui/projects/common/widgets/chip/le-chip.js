import React, { Component } from "common/react-vendor";
import "./le-chip.scss";

class LeChip extends Component {
  constructor(props) {
    super(props);
    this.removeItem = this.removeItem.bind(this);
    this.state = {
      chipsSelected: [{ id: 1, name: "test", displayName: "Test Leo" }]
    };
  }

  removeItem(position) {
    let tmpChips = [...this.state.chipsSelected];
    tmpChips.splice(position, 1);
    this.setState({ chipsSelected: tmpChips });
  }

  addChip(item) {
    let tmpChips = [...this.state.chipsSelected];
    tmpChips.push(item);
    this.setState({ chipsSelected: tmpChips });
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
              this.removeItem(index);
            }}
          />
        </li>
      );
    });
    return chipsUI;
  }

  render() {
    return (
      <div className="le-chips">
        <ul className="le-chips-selected">{this.getChipsSelected()}</ul>
        <div className="le-chip-input">
          <i className="fa fa-search" aria-hidden="true" />
          <input type="text" />
        </div>
      </div>
    );
  }
}

export default LeChip;
