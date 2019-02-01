import React, { Component } from '../../../react-vendor';
import './le-tile.scss';

class LeTile extends Component {
    constructor(props) {
        super(props);
    }
    render() {

        return (
            <div className={`"le-tile" ${this.props.classNames ? this.props.classNames : ''}`}>
                {this.props.children}
            </div>
        );
    }
}

export default LeTile;