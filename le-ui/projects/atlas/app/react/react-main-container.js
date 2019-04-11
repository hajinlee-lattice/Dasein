import React from 'common/react-vendor';
import './react-main.component.scss';


const ReactMainContainer = (props) => {
    
    return(
        <section className={`${'main-content'} ${props.className}`}>
            {props.children}
        </section>
    );
}

export default ReactMainContainer;