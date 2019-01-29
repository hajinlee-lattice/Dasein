import React, { Component } from 'common/react-vendor';
import './le-card.scss';

class LeCard extends Component {
    constructor(props) {
        super(props);
    }
    render() {
        const { children } = this.props;

        const childrenWithProps = React.Children.map(children, child =>
            React.cloneElement(child, { clickHandler: this.clickHandler })
        );

        // return <div>{childrenWithProps}</div>
        return (
            <div className={`${"le-card"} ${this.props.classNames ? this.props.classNames : ''}`} onClick={this.props.clickHandler}>
                {childrenWithProps}
            </div>

        );
    }
}

export default LeCard;