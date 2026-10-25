package com.dataextraction.sink.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Converts Kafka Connect JSON schemas/values (Debezium envelope shape) into Avro schemas and
 * {@link GenericRecord}s for Object Container Files.
 */
public class ConnectAvroSchemas {

    private ObjectMapper objectMapper = new ObjectMapper();    
    private String extractNamespace;

    public ConnectAvroSchemas(String extractNamespace) {
        Objects.requireNonNull(extractNamespace, "extractNamespace is required");
        this.extractNamespace = extractNamespace;
    }

    public Schema extractRecordSchema() {
        return SchemaBuilder.record("extract")
                .namespace(extractNamespace)
                .fields()
                .name("extract_job_id")
                .type()
                .nullable()
                .stringType()
                .noDefault()
                .name("extract_buffer_id")
                .type()
                .nullable()
                .stringType()
                .noDefault()
                .name("extract_type")
                .type()
                .nullable()
                .stringType()
                .noDefault()
                .name("extracted_at")
                .type()
                .nullable()
                .stringType()
                .noDefault()
                .endRecord();
    }

    public Schema topLevelSchema(Schema payloadSchema) {
        Schema extractSchema = extractRecordSchema();
        return SchemaBuilder.record("ExtractRecord")
                .namespace(extractNamespace)
                .fields()
                .name("extract")
                .type(nullable(extractSchema))
                .withDefault(null)
                .name("payload")
                .type(nullable(payloadSchema))
                .withDefault(null)
                .endRecord();
    }

    public Schema payloadSchemaFromConnectJson(String connectSchemaJson) throws IOException {
        if (connectSchemaJson == null || connectSchemaJson.isBlank()) {
            throw new IOException("Connect value schema is required to write Avro extract files");
        }
        JsonNode schemaNode = objectMapper.readTree(connectSchemaJson);
        return toAvroSchema(schemaNode, "payload", new LinkedHashMap<>());
    }

    /** Infer a payload Avro schema from a JSON payload object when Connect schema is absent. */
    public Schema payloadSchemaFromJson(JsonNode payload) {
        if (payload == null || payload.isNull() || payload.isMissingNode()) {
            return SchemaBuilder.record("payload")
                    .namespace(extractNamespace)
                    .fields()
                    .endRecord();
        }
        if (!payload.isObject()) {
            return SchemaBuilder.record("payload")
                    .namespace(extractNamespace)
                    .fields()
                    .name("value")
                    .type()
                    .nullable()
                    .stringType()
                    .noDefault()
                    .endRecord();
        }
        SchemaBuilder.FieldAssembler<Schema> fields =
                SchemaBuilder.record("payload").namespace(extractNamespace).fields();
        Iterator<Map.Entry<String, JsonNode>> it = payload.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            fields = fields.name(entry.getKey()).type(inferAvroType(entry.getValue())).noDefault();
        }
        return fields.endRecord();
    }

    public GenericRecord toTopLevelRecord(Schema topLevelSchema, JsonNode extractNode, JsonNode payloadNode)
            throws IOException {
        GenericRecord record = new GenericData.Record(topLevelSchema);
        Schema extractSchema = nullableBranch(topLevelSchema.getField("extract").schema(), Schema.Type.RECORD);
        Schema payloadSchema = nullableBranch(topLevelSchema.getField("payload").schema(), Schema.Type.RECORD);
        record.put("extract", extractNode == null || extractNode.isNull()
                ? null
                : toRecord(extractSchema, extractNode));
        record.put("payload", payloadNode == null || payloadNode.isNull()
                ? null
                : toRecord(payloadSchema, payloadNode));
        return record;
    }

    private Schema toAvroSchema(JsonNode connectSchema, String defaultName, Map<String, Schema> namedTypes)
            throws IOException {
        if (connectSchema == null || connectSchema.isNull() || connectSchema.isMissingNode()) {
            throw new IOException("Missing Connect schema node");
        }
        String type = textOrNull(connectSchema.get("type"));
        if (type == null) {
            throw new IOException("Connect schema missing type: " + connectSchema);
        }
        boolean optional = connectSchema.path("optional").asBoolean(false);
        Schema base = switch (type) {
            case "int8", "int16", "int32" -> Schema.create(Schema.Type.INT);
            case "int64" -> Schema.create(Schema.Type.LONG);
            case "float", "float32" -> Schema.create(Schema.Type.FLOAT);
            case "double", "float64" -> Schema.create(Schema.Type.DOUBLE);
            case "boolean" -> Schema.create(Schema.Type.BOOLEAN);
            case "string" -> Schema.create(Schema.Type.STRING);
            case "bytes" -> Schema.create(Schema.Type.BYTES);
            case "array" -> Schema.createArray(
                    toAvroSchema(connectSchema.get("items"), defaultName + "_item", namedTypes));
            case "map" -> Schema.createMap(
                    toAvroSchema(connectSchema.get("values"), defaultName + "_value", namedTypes));
            case "struct" -> structToAvro(connectSchema, defaultName, namedTypes);
            default -> throw new IOException("Unsupported Connect type: " + type);
        };
        return optional ? nullable(base) : base;
    }

    private Schema structToAvro(JsonNode connectSchema, String defaultName, Map<String, Schema> namedTypes)
            throws IOException {
        String name = sanitizeName(textOrNull(connectSchema.get("name")), defaultName);
        String namespace = extractNamespace;
        String fullName = namespace + "." + name;
        Schema existing = namedTypes.get(fullName);
        if (existing != null) {
            return existing;
        }

        List<Schema.Field> fields = new ArrayList<>();
        // Placeholder so recursive references resolve.
        Schema recordSchema = Schema.createRecord(name, null, namespace, false);
        namedTypes.put(fullName, recordSchema);

        JsonNode fieldNodes = connectSchema.get("fields");
        if (fieldNodes != null && fieldNodes.isArray()) {
            for (JsonNode fieldNode : fieldNodes) {
                String fieldName = textOrNull(fieldNode.get("field"));
                if (fieldName == null || fieldName.isBlank()) {
                    continue;
                }
                Schema fieldSchema = toAvroSchema(fieldNode, fieldName, namedTypes);
                Object defaultValue = Schema.Field.NULL_DEFAULT_VALUE;
                if (!isNullable(fieldSchema)) {
                    defaultValue = null;
                }
                if (isNullable(fieldSchema)) {
                    fields.add(new Schema.Field(fieldName, fieldSchema, null, defaultValue));
                } else {
                    fields.add(new Schema.Field(fieldName, fieldSchema, null));
                }
            }
        }
        recordSchema.setFields(fields);
        return recordSchema;
    }

    private Schema inferAvroType(JsonNode value) {
        if (value == null || value.isNull()) {
            return nullable(Schema.create(Schema.Type.STRING));
        }
        if (value.isBoolean()) {
            return nullable(Schema.create(Schema.Type.BOOLEAN));
        }
        if (value.isIntegralNumber()) {
            return nullable(Schema.create(Schema.Type.LONG));
        }
        if (value.isFloatingPointNumber()) {
            return nullable(Schema.create(Schema.Type.DOUBLE));
        }
        if (value.isTextual()) {
            return nullable(Schema.create(Schema.Type.STRING));
        }
        if (value.isArray()) {
            Schema element = value.isEmpty()
                    ? Schema.create(Schema.Type.STRING)
                    : nonNullBranch(inferAvroType(value.get(0)));
            return nullable(Schema.createArray(element));
        }
        if (value.isObject()) {
            SchemaBuilder.FieldAssembler<Schema> fields =
                    SchemaBuilder.record("inferred_" + Integer.toHexString(System.identityHashCode(value)))
                            .namespace(extractNamespace)
                            .fields();
            Iterator<Map.Entry<String, JsonNode>> it = value.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                fields = fields.name(entry.getKey()).type(inferAvroType(entry.getValue())).noDefault();
            }
            return nullable(fields.endRecord());
        }
        return nullable(Schema.create(Schema.Type.STRING));
    }

    private GenericRecord toRecord(Schema schema, JsonNode node) throws IOException {
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isObject()) {
            throw new IOException("Expected JSON object for Avro record " + schema.getFullName());
        }
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            JsonNode child = node.get(field.name());
            record.put(field.name(), toAvroValue(field.schema(), child));
        }
        return record;
    }

    private Object toAvroValue(Schema schema, JsonNode node) throws IOException {
        Schema nonNull = nonNullBranch(schema);
        if (node == null || node.isNull() || node.isMissingNode()) {
            if (isNullable(schema)) {
                return null;
            }
            return defaultFor(nonNull);
        }
        return switch (nonNull.getType()) {
            case RECORD -> toRecord(nonNull, node);
            case ARRAY -> {
                List<Object> values = new ArrayList<>();
                for (JsonNode element : node) {
                    values.add(toAvroValue(nonNull.getElementType(), element));
                }
                yield values;
            }
            case MAP -> {
                Map<String, Object> map = new LinkedHashMap<>();
                Iterator<Map.Entry<String, JsonNode>> it = node.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    map.put(entry.getKey(), toAvroValue(nonNull.getValueType(), entry.getValue()));
                }
                yield map;
            }
            case STRING -> node.asText();
            case BYTES -> bytesFromJson(node);
            case INT -> node.intValue();
            case LONG -> node.longValue();
            case FLOAT -> (float) node.doubleValue();
            case DOUBLE -> node.doubleValue();
            case BOOLEAN -> node.booleanValue();
            case ENUM -> new GenericData.EnumSymbol(nonNull, node.asText());
            case UNION -> toAvroValue(nonNull, node);
            case FIXED -> ByteBuffer.wrap(bytesFromJson(node));
            case NULL -> null;
        };
    }

    private byte[] bytesFromJson(JsonNode node) {
        if (node.isBinary()) {
            try {
                return node.binaryValue();
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid binary JSON value", e);
            }
        }
        if (node.isTextual()) {
            String text = node.asText();
            try {
                return Base64.getDecoder().decode(text);
            } catch (IllegalArgumentException ignored) {
                return text.getBytes(StandardCharsets.UTF_8);
            }
        }
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }

    private Object defaultFor(Schema schema) {
        return switch (schema.getType()) {
            case STRING -> "";
            case BOOLEAN -> false;
            case INT -> 0;
            case LONG -> 0L;
            case FLOAT -> 0f;
            case DOUBLE -> 0d;
            case BYTES -> ByteBuffer.wrap(new byte[0]);
            case ARRAY -> List.of();
            case MAP -> Map.of();
            default -> null;
        };
    }

    private Schema nullable(Schema schema) {
        if (isNullable(schema)) {
            return schema;
        }
        return Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), schema));
    }

    private boolean isNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return false;
        }
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() == Schema.Type.NULL) {
                return true;
            }
        }
        return false;
    }

    private Schema nonNullBranch(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                return branch;
            }
        }
        return schema;
    }

    private Schema nullableBranch(Schema schema, Schema.Type type) {
        Schema nonNull = nonNullBranch(schema);
        if (nonNull.getType() == type) {
            return nonNull;
        }
        throw new IllegalArgumentException("Expected " + type + " in schema " + schema);
    }

    private String textOrNull(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        return node.asText();
    }

    private String sanitizeName(String name, String fallback) {
        String raw = (name == null || name.isBlank()) ? fallback : name;
        String sanitized = raw.replaceAll("[^A-Za-z0-9_]", "_");
        if (sanitized.isEmpty() || !Character.isLetter(sanitized.charAt(0))) {
            sanitized = "r_" + sanitized;
        }
        return sanitized.toLowerCase(Locale.ROOT).equals("extractrecord") ? sanitized + "_payload" : sanitized;
    }
}
