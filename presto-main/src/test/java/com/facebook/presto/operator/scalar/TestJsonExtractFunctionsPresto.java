package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.function.JsonPathEngine;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.JsonType.JSON;
import static java.lang.String.format;

public class TestJsonExtractFunctionsPresto extends TestJsonExtractFunctionsBase
{
    protected TestJsonExtractFunctionsPresto()
    {
        super(JsonPathEngine.PRESTO);
    }

    @Test
    public void testJsonExtract()
    {
        testJsonExtractCommon();
    }

    @Test
    public void testJsonSize()
    {
        testJsonSizeCommon();
    }

    @Test
    public void testJsonExtractScalar()
    {
        testJsonExtractScalarCommon();
    }
}
