/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.com)
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.avro.deserialize.visitor;

import io.ballerina.lib.avro.Utils;
import io.ballerina.lib.avro.deserialize.ArrayDeserializer;
import io.ballerina.lib.avro.deserialize.AvroDeserializationException;
import io.ballerina.lib.avro.deserialize.Deserializer;
import io.ballerina.lib.avro.deserialize.EnumDeserializer;
import io.ballerina.lib.avro.deserialize.FixedDeserializer;
import io.ballerina.lib.avro.deserialize.MapDeserializer;
import io.ballerina.lib.avro.deserialize.PrimitiveDeserializer;
import io.ballerina.lib.avro.deserialize.RecordDeserializer;
import io.ballerina.lib.avro.deserialize.UnionDeserializer;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.ReferenceType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.utils.ValueUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.ballerina.lib.avro.Utils.getMutableType;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processArrayField;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processBytesField;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processMapField;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processRecordField;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processStringField;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processUnionField;
import static io.ballerina.runtime.api.utils.StringUtils.fromString;

public class DeserializeVisitor implements IDeserializeVisitor {

    public static Deserializer createDeserializer(Schema schema, Type type) {
        return switch (schema.getElementType().getType()) {
            case UNION ->
                    new UnionDeserializer(type, schema);
            case ARRAY ->
                    new ArrayDeserializer(type, schema);
            case ENUM ->
                    new EnumDeserializer(type, schema);
            case RECORD ->
                    new RecordDeserializer(type, schema);
            case FIXED ->
                    new FixedDeserializer(type, schema);
            default ->
                    new PrimitiveDeserializer(type, schema);
        };
    }

    public BMap<BString, Object> visit(RecordDeserializer recordDeserializer, GenericRecord rec)
            throws AvroDeserializationException {
        Type originalType = recordDeserializer.getType();
        Type type = recordDeserializer.getType();
        Schema schema = recordDeserializer.getSchema();
        BMap<BString, Object> avroRecord = createAvroRecord(type);
        for (Schema.Field field : schema.getFields()) {
            Object fieldData = rec.get(field.name());
            switch (field.schema().getType()) {
                case MAP ->
                        processMapField(avroRecord, field, fieldData);
                case ARRAY -> {
                    Type fieldType = resolveDeclaredFieldType(avroRecord.getType(), field.name());
                    processArrayField(avroRecord, field, fieldData, fieldType);
                }
                case BYTES ->
                        processBytesField(avroRecord, field, fieldData);
                case RECORD ->
                        processRecordField(avroRecord, field, fieldData);
                case STRING ->
                        processStringField(avroRecord, field, fieldData);
                case INT ->
                        avroRecord.put(fromString(field.name()), Long.parseLong(fieldData.toString()));
                case FLOAT ->
                        avroRecord.put(fromString(field.name()), Double.parseDouble(fieldData.toString()));
                case ENUM ->
                        avroRecord.put(fromString(field.name()), fromString(fieldData.toString()));
                case UNION ->
                        processUnionField(type, avroRecord, field, fieldData);
                default ->
                        avroRecord.put(fromString(field.name()), fieldData);
            }
        }

        if (originalType.isReadOnly()) {
            avroRecord.freezeDirect();
        }
        if (type.getTag() == TypeTags.ANYDATA_TAG) {
            return (BMap<BString, Object>) ValueUtils.convert(avroRecord, type);
        }
        return avroRecord;
    }

    public BMap<BString, Object> visit(MapDeserializer mapDeserializer, Map<String, Object> data)
            throws AvroDeserializationException {
        BMap<BString, Object> avroRecord = ValueCreator.createMapValue();
        Object[] keys = data.keySet().toArray();
        Schema schema = mapDeserializer.getSchema();
        Type type = mapDeserializer.getType();
        for (Object key : keys) {
            Object value = data.get(key);
            Schema.Type valueType = schema.getValueType().getType();
            switch (valueType) {
                case ARRAY ->
                        processMapArray(avroRecord, schema,
                                        (MapType) getMutableType(type), key, (GenericData.Array<Object>) value);
                case BYTES ->
                        avroRecord.put(StringUtils.fromString(key.toString()),
                                       ValueCreator.createArrayValue(((ByteBuffer) value).array()));
                case FIXED ->
                        avroRecord.put(StringUtils.fromString(key.toString()),
                                       ValueCreator.createArrayValue(((GenericFixed) value).bytes()));
                case ENUM, STRING ->
                        avroRecord.put(StringUtils.fromString(key.toString()),
                                       StringUtils.fromString(value.toString()));
                case RECORD ->
                        processMapRecord(avroRecord, schema, (MapType) getMutableType(type),
                                         key, (GenericRecord) value);
                case FLOAT ->
                        avroRecord.put(StringUtils.fromString(key.toString()),
                                       Double.parseDouble(value.toString()));
                case MAP ->
                        processMaps(avroRecord, schema, (MapType) getMutableType(type), 
                                    key, (Map<String, Object>) value);
                default ->
                        avroRecord.put(StringUtils.fromString(key.toString()), value);
            }
        }
        return (BMap<BString, Object>) ValueUtils.convert(avroRecord, type);
    }

    public Object visit(PrimitiveDeserializer primitiveDeserializer, Object data) throws AvroDeserializationException {
        Schema schema = primitiveDeserializer.getSchema();
        Type type = primitiveDeserializer.getType();
        switch(schema.getType()) {
            case ARRAY -> {
                return visitPrimitiveArrays(primitiveDeserializer, (GenericData.Array<Object>) data, schema, type);
            }
            case STRING, ENUM -> {
                return StringUtils.fromString(data.toString());
            }
            case FLOAT, DOUBLE -> {
                if (data instanceof Float) {
                    return Double.parseDouble(data.toString());
                }
                return data;
            }
            case NULL -> {
                if (data != null) {
                    throw new AvroDeserializationException("The value does not match with the null schema");
                }
                return null;
            }
            case BYTES -> {
                return ValueCreator.createArrayValue(((ByteBuffer) data).array());
            }
            default -> {
                return data;
            }
        }
    }

    private Object visitPrimitiveArrays(PrimitiveDeserializer primitiveDeserializer, GenericData.Array<Object> data,
                                        Schema schema, Type type) {
        switch (schema.getElementType().getType()) {
            case STRING -> {
                return ValueUtils.convert(visitStringArray(data), type);
            }
            case INT -> {
                return ValueUtils.convert(visitIntArray(data), type);
            }
            case LONG -> {
                return ValueUtils.convert(visitLongArray(data), type);
            }
            case FLOAT, DOUBLE -> {
                return ValueUtils.convert(visitDoubleArray(data), type);
            }
            case BOOLEAN -> {
                return ValueUtils.convert(visitBooleanArray(data), type);
            }
            default -> {
                return ValueUtils.convert(visitBytesArray(data, primitiveDeserializer.getType()), type);
            }
        }
    }

    public BArray visit(UnionDeserializer unionDeserializer, GenericData.Array<Object> data)
            throws AvroDeserializationException {
        Type type = unionDeserializer.getType();
        Schema schema = unionDeserializer.getSchema();
        switch (((ArrayType) type).getElementType().getTag()) {
            case TypeTags.STRING_TAG -> {
                return visitStringArray(data);
            }
            case TypeTags.FLOAT_TAG -> {
                return visitDoubleArray(data);
            }
            case TypeTags.BOOLEAN_TAG -> {
                return visitBooleanArray(data);
            }
            case TypeTags.INT_TAG -> {
                return visitIntegerArray(data, schema);
            }
            case TypeTags.RECORD_TYPE_TAG -> {
                return visitRecordArray(data, type, schema);
            }
            case TypeTags.ARRAY_TAG -> {
                return visitUnionArray(data, (ArrayType) type, schema);
            }
            default -> {
                return (BArray) data;
            }
        }
    }

    private BArray visitRecordArray(GenericData.Array<Object> data, Type type, Schema schema)
            throws AvroDeserializationException {
        RecordDeserializer recordDeserializer = new RecordDeserializer(type, schema.getElementType());
        return (BArray) recordDeserializer.accept(this, data);
    }

    private BArray visitUnionArray(GenericData.Array<Object> data, ArrayType type, Schema schema)
            throws AvroDeserializationException {
        Object[] objects = new Object[data.size()];
        Type elementType = type.getElementType();
        ArrayDeserializer arrayDeserializer = new ArrayDeserializer(elementType, schema.getElementType());
        int index = 0;
        for (Object currentData : data) {
            Object deserializedObject = arrayDeserializer.accept(this, (GenericData.Array<Object>) currentData);
            objects[index++] = deserializedObject;
        }
        return ValueCreator.createArrayValue(objects, type);
    }

    public BArray visit(RecordDeserializer recordDeserializer, GenericData.Array<Object> data)
            throws AvroDeserializationException {
        List<Object> recordList = new ArrayList<>();
        boolean isReadOnly = recordDeserializer.getType().getTag() == TypeTags.INTERSECTION_TAG;
        Type type = Utils.getMutableType(recordDeserializer.getType());
        Schema schema = recordDeserializer.getSchema();
        switch (type.getTag()) {
            case TypeTags.ARRAY_TAG -> {
                for (Object datum : data) {
                    Type fieldType = ((ArrayType) type).getElementType();
                    RecordDeserializer recordDes = new RecordDeserializer(fieldType, schema.getElementType());
                    recordList.add(recordDes.accept(this, datum));
                }
            }
            case TypeTags.TYPE_REFERENCED_TYPE_TAG -> {
                for (Object datum : data) {
                    Type fieldType = ((ReferenceType) type).getReferredType();
                    RecordDeserializer recordDes = new RecordDeserializer(fieldType, schema.getElementType());
                    recordList.add(recordDes.accept(this, (GenericRecord) datum));
                }
            }
        }
        BArray arrayValue = ValueCreator.createArrayValue(recordList.toArray(new Object[data.size()]),
                                                          (ArrayType) type);
        if (isReadOnly) {
            arrayValue.freezeDirect();
        }
        return arrayValue;
    }

    private BMap<BString, Object> createAvroRecord(Type type) {
        if (type.getTag() == TypeTags.ANYDATA_TAG) {
            return ValueCreator.createMapValue();
        }
        return ValueCreator.createRecordValue((RecordType) getMutableType(type));
    }

    private void processMaps(BMap<BString, Object> avroRecord, Schema schema,
                             MapType type, Object key, Map<String, Object> value) throws AvroDeserializationException {
        Schema fieldSchema = schema.getValueType();
        Type fieldType = type.getConstrainedType();
        MapDeserializer mapDes = new MapDeserializer(fieldSchema, fieldType);
        Object fieldValue = mapDes.accept(this, value);
        avroRecord.put(fromString(key.toString()), fieldValue);
    }

    private void processMapRecord(BMap<BString, Object> avroRecord, Schema schema,
                                  MapType type, Object key, GenericRecord value) throws AvroDeserializationException {
        Type fieldType = type.getConstrainedType();
        RecordDeserializer recordDes = new RecordDeserializer(fieldType, schema.getValueType());
        Object fieldValue = recordDes.accept(this, value);
        avroRecord.put(fromString(key.toString()), fieldValue);
    }

    private void processMapArray(BMap<BString, Object> avroRecord, Schema schema,
                                 MapType type, Object key, GenericData.Array<Object> value)
                                         throws AvroDeserializationException {
        Type fieldType = type.getConstrainedType();
        ArrayDeserializer arrayDeserializer = new ArrayDeserializer(fieldType, schema.getValueType());
        Object fieldValue = visit(arrayDeserializer, value);
        avroRecord.put(fromString(key.toString()), fieldValue);
    }

    public Object visit(ArrayDeserializer arrayDeserializer, GenericData.Array<Object> data)
            throws AvroDeserializationException {
        Deserializer deserializer = createDeserializer(arrayDeserializer.getSchema(), arrayDeserializer.getType());
        return deserializer.accept(new DeserializeArrayVisitor(), data);
    }

    public BArray visit(EnumDeserializer enumDeserializer, GenericData.Array<Object> data) {
        Object[] enums = new Object[data.size()];
        for (int i = 0; i < data.size(); i++) {
            enums[i] = visitString(data.get(i));
        }
        return ValueCreator.createArrayValue(enums, (ArrayType) enumDeserializer.getType());
    }

    public Object visit(FixedDeserializer fixedDeserializer, Object data) {
        if (fixedDeserializer.getSchema().getType().equals(Schema.Type.ARRAY)) {
            GenericData.Array<Object> array = (GenericData.Array<Object>) data;
            Type type = fixedDeserializer.getType();
            List<BArray> values = new ArrayList<>();
            for (Object datum : array) {
                values.add(visitFixed(datum));
            }
            return ValueCreator.createArrayValue(values.toArray(new BArray[array.size()]), (ArrayType) type);
        } else {
            return visitFixed(data);
        }
    }

    public BArray visit(FixedDeserializer fixedDeserializer, GenericData.Array<Object> data) {
        Type type = fixedDeserializer.getType();
        List<BArray> values = new ArrayList<>();
        for (Object datum : data) {
            values.add(visitFixed(datum));
        }
        return ValueCreator.createArrayValue(values.toArray(new BArray[data.size()]), (ArrayType) type);
    }

    private static BArray visitIntegerArray(GenericData.Array<Object> data, Schema schema) {
        for (Schema schemaInstance : schema.getElementType().getTypes()) {
            if (schemaInstance.getType().equals(Schema.Type.INT)) {
                return visitIntArray(data);
            }
        }
        return visitLongArray(data);
    }

    private BArray visitBytesArray(GenericData.Array<Object> data, Type type) {
        List<BArray> values = new ArrayList<>();
        for (Object datum : data) {
            values.add(ValueCreator.createArrayValue(((ByteBuffer) datum).array()));
        }
        return ValueCreator.createArrayValue(values.toArray(new BArray[data.size()]), (ArrayType) type);
    }

    private static BArray visitBooleanArray(GenericData.Array<Object> data) {
        boolean[] booleanArray = new boolean[data.size()];
        int index = 0;
        for (Object datum : data) {
            booleanArray[index++] = (boolean) datum;
        }
        return ValueCreator.createArrayValue(booleanArray);
    }

    private BArray visitDoubleArray(GenericData.Array<Object> data) {
        List<Double> doubleList = new ArrayList<>();
        for (Object datum : data) {
            doubleList.add(visitDouble(datum));
        }
        double[] doubleArray = doubleList.stream().mapToDouble(Double::doubleValue).toArray();
        return ValueCreator.createArrayValue(doubleArray);
    }

    private static BArray visitLongArray(GenericData.Array<Object> data) {
        List<Long> longList = new ArrayList<>();
        for (Object datum : data) {
            longList.add((Long) datum);
        }
        long[] longArray = longList.stream().mapToLong(Long::longValue).toArray();
        return ValueCreator.createArrayValue(longArray);
    }

    private static BArray visitIntArray(GenericData.Array<Object> data) {
        List<Long> longList = new ArrayList<>();
        for (Object datum : data) {
            longList.add(((Integer) datum).longValue());
        }
        long[] longArray = longList.stream().mapToLong(Long::longValue).toArray();
        return ValueCreator.createArrayValue(longArray);
    }

    private BArray visitStringArray(GenericData.Array<Object> data) {
        BString[] stringArray = new BString[data.size()];
        for (int i = 0; i < data.size(); i++) {
            stringArray[i] = visitString(data.get(i));
        }
        return ValueCreator.createArrayValue(stringArray);
    }


    public double visitDouble(Object data) {
        if (data instanceof Float) {
            return Double.parseDouble(data.toString());
        }
        return (double) data;
    }

    public BArray visitFixed(Object data) {
        GenericData.Fixed fixed = (GenericData.Fixed) data;
        return ValueCreator.createArrayValue(fixed.bytes());
    }

    public BString visitString(Object data) {
        return fromString(data.toString());
    }

    public static Type extractMapType(Type type, String fieldName) throws AvroDeserializationException {
        Type anydataMapType = TypeCreator.createMapType(PredefinedTypes.TYPE_ANYDATA);
        if (type.getTag() != TypeTags.RECORD_TYPE_TAG) {
            return anydataMapType;
        }
        RecordType recordType = (RecordType) type;
        Field fieldValue = recordType.getFields().get(fieldName);
        if (fieldValue != null) {
            Type resolvedType = resolveFieldType(fieldValue.getFieldType(), TypeTags.MAP_TAG);
            if (resolvedType.getTag() != TypeTags.MAP_TAG) {
                throw new AvroDeserializationException("Field '" + fieldName + "' in type " + type.getName()
                        + " is not a map type.");
            }
            return resolvedType;
        }
        if (recordType.isSealed()) {
            throw new AvroDeserializationException("Field '" + fieldName + "' not found in type " + type.getName());
        }
        return restFieldTypeOrDefault(recordType, fieldName, TypeTags.MAP_TAG, anydataMapType);
    }

    public static Type extractRecordType(Type type, String fieldName) throws AvroDeserializationException {
        if (type.getTag() != TypeTags.RECORD_TYPE_TAG) {
            return PredefinedTypes.TYPE_ANYDATA;
        }
        RecordType recordType = (RecordType) type;
        Field fieldValue = recordType.getFields().get(fieldName);
        if (fieldValue != null) {
            Type resolvedType = resolveFieldType(fieldValue.getFieldType(), TypeTags.RECORD_TYPE_TAG);
            if (resolvedType.getTag() != TypeTags.RECORD_TYPE_TAG) {
                throw new AvroDeserializationException("Field '" + fieldName + "' in type " + type.getName()
                        + " is not a record type.");
            }
            return resolvedType;
        }
        if (recordType.isSealed()) {
            throw new AvroDeserializationException("Field '" + fieldName + "' not found in type " + type.getName());
        }
        return restFieldTypeOrDefault(recordType, fieldName, TypeTags.RECORD_TYPE_TAG, recordType);
    }

    private static Type resolveDeclaredFieldType(Type type, String fieldName) throws AvroDeserializationException {
        if (type.getTag() != TypeTags.RECORD_TYPE_TAG) {
            return PredefinedTypes.TYPE_ANYDATA_ARRAY;
        }
        RecordType recordType = (RecordType) type;
        Field fieldValue = recordType.getFields().get(fieldName);
        if (fieldValue != null) {
            return fieldValue.getFieldType();
        }
        if (recordType.isSealed()) {
            throw new AvroDeserializationException("Field '" + fieldName + "' not found in type " + type.getName());
        }
        return restFieldTypeOrDefault(recordType, fieldName, TypeTags.ARRAY_TAG, PredefinedTypes.TYPE_ANYDATA_ARRAY);
    }

    private static Type restFieldTypeOrDefault(RecordType recordType, String fieldName, int desiredTag,
            Type permissiveDefault) throws AvroDeserializationException {
        Type restFieldType = recordType.getRestFieldType();
        if (restFieldType == null || restFieldType.getTag() == TypeTags.ANYDATA_TAG) {
            return permissiveDefault;
        }
        Type resolvedRestType = resolveFieldType(restFieldType, desiredTag);
        if (resolvedRestType.getTag() != desiredTag) {
            throw new AvroDeserializationException("Field '" + fieldName + "' in type " + recordType.getName()
                    + " is not compatible with its declared rest field type.");
        }
        return resolvedRestType;
    }

    private static Type resolveFieldType(Type fieldType, int desiredTag) {
        if (fieldType.getTag() == desiredTag) {
            return fieldType;
        }
        if (fieldType.getTag() == TypeTags.INTERSECTION_TAG) {
            return resolveFieldType(getMutableType(fieldType), desiredTag);
        }
        if (fieldType.getTag() == TypeTags.UNION_TAG) {
            for (Type memberType : ((UnionType) fieldType).getMemberTypes()) {
                Type resolvedMember = resolveFieldType(memberType, desiredTag);
                if (resolvedMember.getTag() == desiredTag) {
                    return resolvedMember;
                }
            }
            return fieldType;
        }
        Type referredType = TypeUtils.getReferredType(fieldType);
        if (referredType.getTag() == desiredTag || referredType.getTag() == TypeTags.UNION_TAG
                || referredType.getTag() == TypeTags.INTERSECTION_TAG) {
            return resolveFieldType(referredType, desiredTag);
        }
        return fieldType;
    }
}
