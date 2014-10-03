package org.apache.avro.generic;

import java.io.IOException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.data.RecordBuilderBase;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericModifiableData.ModifiableRecord;

/**
 * A RecordBuilder for generic records. ModifiableRecordBuilder fills in default
 * values for fields if they are not specified.
 */
public class ModifiableRecordBuilder extends RecordBuilderBase<Record> {
    private GenericModifiableData.ModifiableRecord record;

    /**
     * Creates a ModifiableRecordBuilder for building Record instances.
     * 
     * @param schema
     *            the schema associated with the record class.
     */
    public ModifiableRecordBuilder(Schema schema) {
        super(schema, GenericData.get());
        record = new ModifiableRecord(schema);
    }

    /**
     * Gets the value of a field.
     * 
     * @param fieldName
     *            the name of the field to get.
     * @return the value of the field with the given name, or null if not set.
     */
    public Object get(String fieldName) {
        return get(schema().getField(fieldName));
    }

    /**
     * Gets the value of a field.
     * 
     * @param field
     *            the field to get.
     * @return the value of the given field, or null if not set.
     */
    public Object get(Field field) {
        return get(field.pos());
    }

    /**
     * Gets the value of a field.
     * 
     * @param pos
     *            the position of the field to get.
     * @return the value of the field with the given position, or null if not
     *         set.
     */
    protected Object get(int pos) {
        return record.get(pos);
    }

    /**
     * Sets the value of a field.
     * 
     * @param fieldName
     *            the name of the field to set.
     * @param value
     *            the value to set.
     * @return a reference to the RecordBuilder.
     */
    public ModifiableRecordBuilder set(String fieldName, Object value) {
        return set(schema().getField(fieldName), value);
    }

    /**
     * Sets the value of a field.
     * 
     * @param field
     *            the field to set.
     * @param value
     *            the value to set.
     * @return a reference to the RecordBuilder.
     */
    public ModifiableRecordBuilder set(Field field, Object value) {
        return set(field, field.pos(), value);
    }

    /**
     * Sets the value of a field.
     * 
     * @param pos
     *            the field to set.
     * @param value
     *            the value to set.
     * @return a reference to the RecordBuilder.
     */
    protected ModifiableRecordBuilder set(int pos, Object value) {
        return set(fields()[pos], pos, value);
    }

    /**
     * Sets the value of a field.
     * 
     * @param field
     *            the field to set.
     * @param pos
     *            the position of the field.
     * @param value
     *            the value to set.
     * @return a reference to the RecordBuilder.
     */
    private ModifiableRecordBuilder set(Field field, int pos, Object value) {
        validate(field, value);
        record.put(pos, value);
        fieldSetFlags()[pos] = true;
        return this;
    }

    /**
     * Checks whether a field has been set.
     * 
     * @param fieldName
     *            the name of the field to check.
     * @return true if the given field is non-null; false otherwise.
     */
    public boolean has(String fieldName) {
        return has(schema().getField(fieldName));
    }

    /**
     * Checks whether a field has been set.
     * 
     * @param field
     *            the field to check.
     * @return true if the given field is non-null; false otherwise.
     */
    public boolean has(Field field) {
        return has(field.pos());
    }

    /**
     * Checks whether a field has been set.
     * 
     * @param pos
     *            the position of the field to check.
     * @return true if the given field is non-null; false otherwise.
     */
    protected boolean has(int pos) {
        return fieldSetFlags()[pos];
    }

    /**
     * Clears the value of the given field.
     * 
     * @param fieldName
     *            the name of the field to clear.
     * @return a reference to the RecordBuilder.
     */
    public ModifiableRecordBuilder clear(String fieldName) {
        return clear(schema().getField(fieldName));
    }

    /**
     * Clears the value of the given field.
     * 
     * @param field
     *            the field to clear.
     * @return a reference to the RecordBuilder.
     */
    public ModifiableRecordBuilder clear(Field field) {
        return clear(field.pos());
    }

    /**
     * Clears the value of the given field.
     * 
     * @param pos
     *            the position of the field to clear.
     * @return a reference to the RecordBuilder.
     */
    protected ModifiableRecordBuilder clear(int pos) {
        record.put(pos, null);
        fieldSetFlags()[pos] = false;
        return this;
    }

    @Override
    public Record build() {
        Record record;
        try {
            record = new GenericData.Record(schema());
        } catch (Exception e) {
            throw new AvroRuntimeException(e);
        }

        for (Field field : fields()) {
            Object value;
            try {
                value = getWithDefault(field);
            } catch (IOException e) {
                throw new AvroRuntimeException(e);
            }
            if (value != null) {
                record.put(field.pos(), value);
            }
        }

        return record;
    }

    /**
     * Gets the value of the given field. If the field has been set, the set
     * value is returned (even if it's null). If the field hasn't been set and
     * has a default value, the default value is returned.
     * 
     * @param field
     *            the field whose value should be retrieved.
     * @return the value set for the given field, the field's default value, or
     *         null.
     * @throws IOException
     */
    private Object getWithDefault(Field field) throws IOException {
        return fieldSetFlags()[field.pos()] ? record.get(field.pos()) : defaultValue(field);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((record == null) ? 0 : record.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        ModifiableRecordBuilder other = (ModifiableRecordBuilder) obj;
        if (record == null) {
            if (other.record != null)
                return false;
        } else if (!record.equals(other.record))
            return false;
        return true;
    }
}
