package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.function.JsonPathEngine;
import org.testng.annotations.Test;

public class TestJsonExtractFunctionsJayway
        extends TestJsonExtractFunctionsBase
{
    protected TestJsonExtractFunctionsJayway()
    {
        super(JsonPathEngine.JAYWAY);
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
}
