package com.latticeengines.liaison.exposed.service;

import com.latticeengines.liaison.exposed.exception.BadMetadataValueException;
import com.latticeengines.liaison.exposed.exception.DefinitionException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class QueryColumn {

    private String name;
    private String expression;
    private Map<String,String> metadata;
    
    //private static final int maxDisplayNameLength = 64;
    private static final int maxDisplayNameLength = 256;
    private static final int maxCategoryLength = 22;
    private static final int maxDescriptionLength = 150;
    private static final Set<String> approvedUsageValues = new HashSet<String>( Arrays.asList("None","Model","ModelAndModelInsights","ModelAndAllInsights") );
    private static final Set<String> statisticalTypeValues = new HashSet<String>( Arrays.asList("nominal","ratio","ordinal","interval") );
    private static final Set<String> tagsValues = new HashSet<String>( Arrays.asList("Internal","External") );
    private static final Set<String> fundamentalTypeValues = new HashSet<String>( Arrays.asList("unknown","nominal","float","alpha","numeric","currency","percentage","boolean","year") );
    
    
    public QueryColumn( String definition ) throws DefinitionException {
        initFromDefinition( definition );
    }
    
    public QueryColumn( String name, String expression, Map<String,String> metadata ) {
        initFromValues( name, expression, metadata );
    }
    
    public QueryColumn( QueryColumn other ) {
        name = other.name;
        expression = other.expression;
        metadata = new HashMap<>( other.metadata );
    }
    
    public String getName() {
        return name;
    }
    
    public void setName( String name ) {
        this.name = name;
    }
    
    public String getExpression() {
        return expression;
    }
    
    public void setExpression( String expression ) {
        this.expression = expression;
    }
    
    public void setApprovedUsage( String value ) throws BadMetadataValueException {
        if( !approvedUsageValues.contains(value) ) {
            throw new BadMetadataValueException( String.format("Cannot set ApprovedUsage to %s", value) );
        }
        metadata.put( "ApprovedUsage", value );
    }
    
    public void setDisplayName( String value ) throws BadMetadataValueException {
        if( value.length() > maxDisplayNameLength ) {
            throw new BadMetadataValueException( String.format("DisplayName \'%s\' is longer than %d", value, maxDisplayNameLength) );
        }
        metadata.put( "DisplayName", value );
    }
    
    public void setCategory( String value ) throws BadMetadataValueException {
        if( value.length() > maxCategoryLength ) {
            throw new BadMetadataValueException( String.format("Category \'%s\' is longer than %d", value, maxCategoryLength) );
        }
        metadata.put( "Category", value );
    }
    
    public void setStatisticalType( String value ) throws BadMetadataValueException {
        if( !statisticalTypeValues.contains(value.toLowerCase()) ) {
            throw new BadMetadataValueException( String.format("Cannot set StatisticalType to %s", value) );
        }
        metadata.put( "StatisticalType", value.toLowerCase() );
    }
    
    public void setTags( String value ) throws BadMetadataValueException {
        if( !tagsValues.contains(value) ) {
            throw new BadMetadataValueException( String.format("Cannot set Tags to %s", value) );
        }
        metadata.put( "Tags", value );
    }
    
    public void setFundamentalType( String value ) throws BadMetadataValueException {
        if( !fundamentalTypeValues.contains(value.toLowerCase()) ) {
            throw new BadMetadataValueException( String.format("Cannot set FundamentalType to %s", value) );
        }
        if( value.equals("Unknown") ) {
            metadata.put( "FundamentalType", value );
        }
        else {
            metadata.put( "FundamentalType", value.toLowerCase() );
        }
    }
    
    public void setDescription( String value ) throws BadMetadataValueException {
        if( value.length() > maxDescriptionLength ) {
            throw new BadMetadataValueException( String.format("Description \'%s\' is longer than %d", value, maxDescriptionLength) );
        }
        metadata.put( "Description", value );
    }
    
    public void setDisplayDiscretization( String value ) {
        metadata.put( "DisplayDiscretizationStrategy", value );
    }
    
    public void setMetadata( Map<String,String> metadata ) throws BadMetadataValueException {
        for( Map.Entry<String,String> entry : metadata.entrySet() ) {
            setMetadata( entry.getKey(), entry.getValue() );
        }
    }
    
    public void setMetadata( String metadataType, String metadataValue ) throws BadMetadataValueException {
        if( metadataType.equals("ApprovedUsage") ) {
            setApprovedUsage( metadataValue );
        }
        else if ( metadataType.equals("DisplayName") ) {
            setDisplayName( metadataValue );
        }
        else if ( metadataType.equals("Category") ) {
            setCategory( metadataValue );
        }
        else if ( metadataType.equals("StatisticalType") ) {
            setStatisticalType( metadataValue );
        }
        else if ( metadataType.equals("Tags") ) {
            setTags( metadataValue );
        }
        else if ( metadataType.equals("FundamentalType") ) {
            setFundamentalType( metadataValue );
        }
        else if ( metadataType.equals("Description") ) {
            setDescription( metadataValue );
        }
        else if ( metadataType.equals("DisplayDiscretizationStrategy") ) {
            setDisplayDiscretization( metadataValue );
        }
        else {
            throw new BadMetadataValueException( String.format("\"%s\" is not a valid metadata type", metadataType) );
        }
    }
    
    public Map<String,String> getMetadata() {
        return metadata;
    }
    
    public void initFromValues( String name, String expression, Map<String,String> metadata ) {
        this.name = name;
        this.expression = expression;
        this.metadata = new HashMap<>();
        setMetadata( metadata );
        if ( !this.metadata.containsKey("Tags") ) {
            setTags("Internal");
        }
        if ( this.metadata.get("Tags").equals("Internal") && !this.metadata.containsKey("Category") ) {
            setCategory("Lead Information");
        }
    }
    
    public abstract void initFromDefinition( String defn ) throws DefinitionException;
    
    public abstract String definition();
}
