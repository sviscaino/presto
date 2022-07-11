package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.function.JsonPathEngine;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public abstract class TestJsonExtractFunctionsBase extends AbstractTestFunctions
{
    protected TestJsonExtractFunctionsBase(JsonPathEngine jsonPathEngine)
    {
        super(testSessionBuilder()
                .setSystemProperty("jsonpath_extraction_engine", String.valueOf(jsonPathEngine))
                .build());
    }

    protected void testJsonExtractCommon() {
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), JSON, "{\"x\":{\"a\":1,\"b\":2}}");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), JSON, "{\"a\":1,\"b\":2}");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), JSON, "1");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [2, 3]} }", "$.x.b[1]"), JSON, "3");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "[1,2,3]", "$[1]"), JSON, "2");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "INVALID_JSON", "$"), JSON, null);
        assertInvalidFunction(format("JSON_EXTRACT('%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");
    }

    protected void testJsonExtractScalarCommon() {
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), VARCHAR, "1");
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [2, 3]} }", "$.x.b[1]"), VARCHAR, "3");
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "[1,2,3]", "$[1]"), VARCHAR, "2");
        assertInvalidFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");
    }

    protected void testJsonSizeCommon() {
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), BIGINT, 1L);
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), BIGINT, 2L);
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [1,2,3], \"c\" : {\"w\":9}} }", "$.x"), BIGINT, 3L);
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), BIGINT, 0L);
        assertFunction(format("JSON_SIZE('%s', '%s')", "[1,2,3]", "$"), BIGINT, 3L);
        assertFunction(format("JSON_SIZE('%s', CHAR '%s')", "[1,2,3]", "$"), BIGINT, 3L);
        assertFunction(format("JSON_SIZE(null, '%s')", "$"), BIGINT, null);
        assertFunction(format("JSON_SIZE('%s', '%s')", "INVALID_JSON", "$"), BIGINT, null);
        assertFunction(format("JSON_SIZE('%s', null)", "[1,2,3]"), BIGINT, null);
        assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), BIGINT, 1L);
        assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), BIGINT, 2L);
        assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [1,2,3], \"c\" : {\"w\":9}} }", "$.x"), BIGINT, 3L);
        assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), BIGINT, 0L);
        assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "[1,2,3]", "$"), BIGINT, 3L);
        assertFunction(format("JSON_SIZE(null, '%s')", "$"), BIGINT, null);
        assertFunction(format("JSON_SIZE(JSON '%s', null)", "[1,2,3]"), BIGINT, null);
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");
        assertInvalidFunction(format("JSON_SIZE('%s', CHAR '%s')", "{\"\":\"\"}", " "), "Invalid JSON path: ' '");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", "."), "Invalid JSON path: '.'");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", "null"), "Invalid JSON path: 'null'");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", null), "Invalid JSON path: 'null'");
    }
}
