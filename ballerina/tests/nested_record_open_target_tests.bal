// Copyright (c) 2026 WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/test;

// A fully open/dynamic target type, e.g. a connector decoding a schema that
// is only known at runtime - zero declared fields, on purpose.
public type OpenPayload record {};

@test:Config {
    groups: ["record", "union"]
}
public isolated function testNestedRecordDecodedIntoOpenParentType() returns error? {
    string schema = string `
        {"type":"record","name":"Outer","fields":[
          {"name":"header","type":{"type":"record","name":"Header","fields":[
            {"name":"entityName","type":"string"}
          ]}}
        ]}`;

    Schema avro = check new (schema);
    OpenPayload event = {"header": {"entityName": "Account"}};
    byte[] serializedValue = check avro.toAvro(event);
    OpenPayload decoded = check avro.fromAvro(serializedValue);
    test:assertEquals(decoded, event);
}

// Sanity check: the same schema/payload decodes fine when the target type
// explicitly declares the nested field with a concrete record type - this
// isolates the bug to open/dynamic parent types, not nested records in
// general.
public type ClosedHeader record {
    string entityName;
};

public type ClosedOuter record {
    ClosedHeader header;
};

@test:Config {
    groups: ["record"]
}
public isolated function testNestedRecordDecodedIntoClosedParentTypeStillWorks() returns error? {
    string schema = string `
        {"type":"record","name":"Outer","fields":[
          {"name":"header","type":{"type":"record","name":"Header","fields":[
            {"name":"entityName","type":"string"}
          ]}}
        ]}`;

    ClosedOuter event = {
        header: {entityName: "Account"}
    };

    return verifyOperation(ClosedOuter, event, schema);
}

// A CDC-shaped repro: an array nested inside a sub-record, decoded into an
// open parent type.
public type ArrayHeaderForOpenTarget record {
    string[] changedFields;
};

public type ClosedArrayOuter record {
    ArrayHeaderForOpenTarget header;
};

@test:Config {
    groups: ["record", "array", "union"]
}
public isolated function testArrayFieldNestedInSubRecordOfOpenParentType() returns error? {
    string schema = string `
        {"type":"record","name":"OpenArrayOuter","fields":[
          {"name":"header","type":{"type":"record","name":"ArrayHeader","fields":[
            {"name":"changedFields","type":{"type":"array","items":"string"}}
          ]}}
        ]}`;

    Schema avro = check new (schema);

    // Encode via a concretely-typed value (the wire bytes Salesforce would
    // actually send) so this test isolates the decode-side regression, not
    // the unrelated pre-existing toAvro-with-untyped-array quirk.
    ClosedArrayOuter typedEvent = {
        header: {changedFields: ["Name", "Phone"]}
    };
    byte[] serializedValue = check avro.toAvro(typedEvent);

    OpenPayload decoded = check avro.fromAvro(serializedValue);
    OpenPayload expected = {"header": {"changedFields": ["Name", "Phone"]}};
    test:assertEquals(decoded, expected);
}

// A scalar field nested inside a union-wrapped sub-record, decoded into an
// open parent type. Reproduces a non-Utf8-backed union string (schema
// property "avro.java.string": "String") the way a producer other than this
// module's own serializer can emit it.
public type ScalarHeaderForOpenTarget record {
    string entityName;
    string? changeType;
    int commitTimestamp;
};

public type UnionWrappedOuter record {
    ScalarHeaderForOpenTarget? header;
};

@test:Config {
    groups: ["record", "union"]
}
public isolated function testNonUtf8ScalarFieldInUnionWrappedSubRecordOfOpenParentType() returns error? {
    string schema = string `
        {"type":"record","name":"UnionWrappedOuter","fields":[
          {"name":"header","type":["null",{"type":"record","name":"ScalarHeader","fields":[
            {"name":"entityName","type":"string"},
            {"name":"changeType","type":["null",{"type":"string","avro.java.string":"String"}],"default":null},
            {"name":"commitTimestamp","type":"long"}
          ]}],"default":null}
        ]}`;

    Schema avro = check new (schema);
    UnionWrappedOuter typedEvent = {
        header: {entityName: "Account", changeType: "CREATE", commitTimestamp: 1726500000000}
    };
    byte[] encoded = check avro.toAvro(typedEvent);

    OpenPayload decoded = check avro.fromAvro(encoded);
    map<anydata> header = <map<anydata>>decoded["header"];
    string changeType = <string>header["changeType"];
    test:assertEquals(changeType, "CREATE");
}

// A directly-typed (non-union, required) enum field nested inside a
// required sub-record, decoded into an open parent type. Reproduces
// Salesforce Pub/Sub CDC's actual ChangeEventHeader.changeType shape: both
// the header record and its changeType field are required, not union
// members, so this never goes through visitUnionRecords at all -- it hits
// the top-level per-field switch in DeserializeVisitor.visit(RecordDeserializer,
// GenericRecord), which previously had no ENUM case and fell through to the
// default branch, inserting the raw org.apache.avro.generic.GenericData
// .EnumSymbol instead of converting it to a Ballerina string.
public type DirectEnumHeaderForOpenTarget record {
    string entityName;
    Numbers changeType;
};

public type DirectEnumOuter record {
    DirectEnumHeaderForOpenTarget header;
};

@test:Config {
    groups: ["record", "enum"]
}
public isolated function testDirectEnumFieldNestedInRequiredSubRecordOfOpenParentType() returns error? {
    string schema = string `
        {"type":"record","name":"DirectEnumOuter","fields":[
          {"name":"header","type":{"type":"record","name":"DirectEnumHeader","fields":[
            {"name":"entityName","type":"string"},
            {"name":"changeType","type":{"type":"enum","name":"Numbers","symbols":["ONE","TWO","THREE","FOUR"]}}
          ]}}
        ]}`;

    Schema avro = check new (schema);
    DirectEnumOuter typedEvent = {
        header: {entityName: "Account", changeType: TWO}
    };
    byte[] encoded = check avro.toAvro(typedEvent);

    OpenPayload decoded = check avro.fromAvro(encoded);
    map<anydata> header = <map<anydata>>decoded["header"];
    string changeType = <string>header["changeType"];
    test:assertEquals(changeType, "TWO");
}
