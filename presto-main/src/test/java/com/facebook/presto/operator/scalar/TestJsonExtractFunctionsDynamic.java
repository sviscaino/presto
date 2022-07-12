/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.function.JsonPathExtractionEngine;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class TestJsonExtractFunctionsDynamic
        extends TestJsonExtractFunctionsBase
{
    protected TestJsonExtractFunctionsDynamic()
    {
        super(JsonPathExtractionEngine.DYNAMIC);
    }

    private String json = "{\n" +
            "    \"store\": {\n" +
            "        \"book\": [\n" +
            "            {\n" +
            "                \"category\": \"reference\",\n" +
            "                \"author\": \"Nigel Rees\",\n" +
            "                \"title\": \"Sayings of the Century\",\n" +
            "                \"price\": 8.95\n" +
            "            },\n" +
            "            {\n" +
            "                \"category\": \"fiction\",\n" +
            "                \"author\": \"Evelyn Waugh\",\n" +
            "                \"title\": \"Sword of Honour\",\n" +
            "                \"price\": 12.99\n" +
            "            },\n" +
            "            {\n" +
            "                \"category\": \"fiction\",\n" +
            "                \"author\": \"Herman Melville\",\n" +
            "                \"title\": \"Moby Dick\",\n" +
            "                \"isbn\": \"0-553-21311-3\",\n" +
            "                \"price\": 8.99\n" +
            "            },\n" +
            "            {\n" +
            "                \"category\": \"fiction\",\n" +
            "                \"author\": \"J. R. R. Tolkien\",\n" +
            "                \"title\": \"The Lord of the Rings\",\n" +
            "                \"isbn\": \"0-395-19395-8\",\n" +
            "                \"price\": 22.99\n" +
            "            }\n" +
            "        ],\n" +
            "        \"bicycle\": {\n" +
            "            \"color\": \"red\",\n" +
            "            \"price\": 19.95\n" +
            "        }\n" +
            "    },\n" +
            "    \"expensive\": 10\n" +
            "}";

    @Test
    public void testJsonExtract()
    {
        testJsonExtractCommon();

        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "$.store.book[*].isbn"), JSON, "[\"0-553-21311-3\",\"0-395-19395-8\"]");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "$..price"), JSON, "[8.95,12.99,8.99,22.99,19.95]");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "$.store.book[?(@.price < 10)].title"), JSON, "[\"Sayings of the Century\",\"Moby Dick\"]");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "max($..price)"), JSON, "22.99");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "concat($..category)"), JSON, "\"referencefictionfictionfiction\"");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "$.store.keys()"), JSON, "[\"book\",\"bicycle\"]");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "$.store.book[1].author"), JSON, "\"Evelyn Waugh\"");

        assertInvalidFunction(format("JSON_EXTRACT('%s', '%s')", json, "$...invalid"), "Invalid JSON path: '$...invalid'");
    }

    @Test
    public void testJsonSize()
    {
        testJsonSizeCommon();

        assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.book[*].isbn"), BIGINT, 2L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "$..price"), BIGINT, 5L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.book[?(@.price < 10)].title"), BIGINT, 2L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "max($..price)"), BIGINT, 0L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "concat($..category)"), BIGINT, 0L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.keys()"), BIGINT, 2L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.book[1].author"), BIGINT, 0L);

        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", json, "$...invalid"), "Invalid JSON path: '$...invalid'");
    }

    @Test
    public void testJsonExtractScalar()
    {
        testJsonExtractScalarCommon();

        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.book[*].isbn"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$..price"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.book[?(@.price < 10)].title"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "max($..price)"), VARCHAR, "22.99");
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "concat($..category)"), VARCHAR, "referencefictionfictionfiction");
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.keys()"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.book[1].author"), VARCHAR, "Evelyn Waugh");

        assertInvalidFunction(format("JSON_EXTRACT_SCALAR('%s', '%s')", json, "$...invalid"), "Invalid JSON path: '$...invalid'");
    }
}
