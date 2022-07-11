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

import io.airlift.slice.Slice;

public class JsonPath
{
    private JsonExtract.JsonExtractor<Slice> scalarExtractor;
    private JsonExtract.JsonExtractor<Slice> objectExtractor;
    private JsonExtract.JsonExtractor<Long> sizeExtractor;
    private final String pattern;

    public JsonPath(String pattern)
    {
        this.pattern = pattern;
    }

    public String getPattern()
    {
        return this.pattern;
    }

    public JsonExtract.JsonExtractor<Slice> getScalarExtractor()
    {
        if (scalarExtractor == null) {
            scalarExtractor = JsonExtract.generateExtractor(pattern, new JsonExtract.ScalarValueJsonExtractor());
        }
        return scalarExtractor;
    }

    public JsonExtract.JsonExtractor<Slice> getObjectExtractor()
    {
        if (objectExtractor == null) {
            objectExtractor = JsonExtract.generateExtractor(pattern, new JsonExtract.JsonValueJsonExtractor());
        }
        return objectExtractor;
    }

    public JsonExtract.JsonExtractor<Long> getSizeExtractor()
    {
        if (sizeExtractor == null) {
            sizeExtractor = JsonExtract.generateExtractor(pattern, new JsonExtract.JsonSizeExtractor());
        }
        return sizeExtractor;
    }
}
