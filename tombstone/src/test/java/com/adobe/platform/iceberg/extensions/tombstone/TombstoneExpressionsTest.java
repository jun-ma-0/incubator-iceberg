/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.adobe.platform.iceberg.extensions.tombstone;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.expressions.Expression;
import org.junit.Assert;
import org.junit.Test;

public class TombstoneExpressionsTest {

  @Test
  public void notIn() {
    List<ExtendedEntry> tbs = Lists.newArrayList(
        new ExtendedEntry.Builder().withEmptyProperties(() -> "a"),
        new ExtendedEntry.Builder().withEmptyProperties(() -> "b"),
        new ExtendedEntry.Builder().withEmptyProperties(() -> "c"),
        new ExtendedEntry.Builder().withEmptyProperties(() -> "d")
    );

    Optional<Expression> optExp = TombstoneExpressions.notIn("fld", tbs);
    Assert.assertTrue(optExp.isPresent());
    Expression exp = optExp.get();
    Assert.assertEquals("(((ref(name=\"fld\") != \"a\" and ref(name=\"fld\") != \"b\") " +
        "and ref(name=\"fld\") != \"c\") and ref(name=\"fld\") != \"d\")", exp.toString());

    Optional<Expression> optExp2 = TombstoneExpressions.notIn("fld", new ArrayList<ExtendedEntry>());
    Assert.assertFalse(optExp2.isPresent());
  }

  @Test
  public void matchesAny() {
    Optional<Expression> optExp = TombstoneExpressions.matchesAny("fld2",
        Lists.newArrayList("m", "n", "o", "p"));
    Assert.assertTrue(optExp.isPresent());
    Expression exp = optExp.get();
    Assert.assertEquals("(((ref(name=\"fld2\") == \"m\" or ref(name=\"fld2\") == \"n\") " +
        "or ref(name=\"fld2\") == \"o\") or ref(name=\"fld2\") == \"p\")", exp.toString());
    Optional<Expression> optExp2 = TombstoneExpressions.matchesAny("fld", new ArrayList<String>());
    Assert.assertFalse(optExp2.isPresent());
  }
}
