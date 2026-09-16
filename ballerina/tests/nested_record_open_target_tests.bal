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

public type CdcHeaderForOpenTarget record {
    string entityName;
    string changeType;
    string[] recordIds;
    string[] changedFields;
    string[] nulledFields;
    string[] diffFields;
    int commitTimestamp;
};

public type CdcEventForOpenTarget record {
    CdcHeaderForOpenTarget ChangeEventHeader;
    string? Name;
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

// Salesforce CDC uses a richer ChangeEventHeader than the minimal array
// repro above. Decode it into `record {}` to ensure all header scalars and
// bitmap arrays are materialized as ordinary Ballerina values.
@test:Config {
    groups: ["record", "array", "union"]
}
public isolated function testSalesforceCdcHeaderDecodesIntoOpenPayload() returns error? {
    string schema = string `
        {"type":"record","name":"AccountChangeEvent","fields":[
          {"name":"ChangeEventHeader","type":{"type":"record","name":"ChangeEventHeader","fields":[
            {"name":"entityName","type":"string"},
            {"name":"changeType","type":["null","string"],"default":null},
            {"name":"recordIds","type":{"type":"array","items":"string"}},
            {"name":"changedFields","type":{"type":"array","items":"string"}},
            {"name":"nulledFields","type":{"type":"array","items":"string"}},
            {"name":"diffFields","type":{"type":"array","items":"string"}},
            {"name":"commitTimestamp","type":"long"}
          ]}},
          {"name":"Name","type":["null","string"],"default":null}
        ]}`;

    Schema avro = check new (schema);
    CdcHeaderForOpenTarget header = {
        entityName: "Account", changeType: "CREATE", recordIds: ["001000000000001"],
        changedFields: ["0x02"], nulledFields: [], diffFields: [], commitTimestamp: 1726500000000
    };
    CdcEventForOpenTarget event = {
        ChangeEventHeader: header, Name: "CDC regression account"
    };
    byte[] encoded = check avro.toAvro(event);

    OpenPayload decoded = check avro.fromAvro(encoded);
    test:assertEquals(decoded, <OpenPayload>{"ChangeEventHeader": header, "Name": "CDC regression account"});
}
