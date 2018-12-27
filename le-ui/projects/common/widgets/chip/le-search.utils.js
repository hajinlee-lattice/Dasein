const search = (list, filter, fieldName) => {
    if(list){
        let result = list.filter(obj => {
            return obj[fieldName].toLowerCase().includes(filter.toLowerCase());
        });
        // console.log('RESULT ', result);
        return result;
    }else{
        return [];
    }
};

export default search;