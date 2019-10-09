/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.batch.http.to.hdfs;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.ws.rs.HttpMethod;

public class HTTPToHDFSActionConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final HTTPToHDFSActionConfig VALID_CONFIG = new HTTPToHDFSActionConfig(
    "/test/path",
    "http://test-url",
    "GET",
    "test_body",
    null,
    "Text",
    "UTF-8",
    true,
    false,
    0,
    60 * 1000,
    60 * 1000,
    "filePath",
    "responseHeaders"
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateURL() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HTTPToHDFSActionConfig config = HTTPToHDFSActionConfig.builder(VALID_CONFIG)
      .setUrl("test_url")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(HTTPToHDFSActionConfig.URL)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateReadTimeout() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HTTPToHDFSActionConfig config = HTTPToHDFSActionConfig.builder(VALID_CONFIG)
      .setReadTimeout(-1)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(HTTPToHDFSActionConfig.READ_TIMEOUT)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateConnectTimeout() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HTTPToHDFSActionConfig config = HTTPToHDFSActionConfig.builder(VALID_CONFIG)
      .setConnectTimeout(-1)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(HTTPToHDFSActionConfig.CONNECT_TIMEOUT)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateRequestHeaders() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HTTPToHDFSActionConfig config = HTTPToHDFSActionConfig.builder(VALID_CONFIG)
      .setRequestHeaders("Test")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(HTTPToHDFSActionConfig.REQUEST_HEADERS)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateMethod() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    HTTPToHDFSActionConfig config = HTTPToHDFSActionConfig.builder(VALID_CONFIG)
      .setMethod(HttpMethod.PUT)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(HTTPToHDFSActionConfig.METHOD)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  private void assertValidationFailed(MockFailureCollector failureCollector, List<List<String>> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(paramNames.size(), failureList.size());
    Iterator<List<String>> paramNameIterator = paramNames.iterator();
    failureList.stream().map(failure -> failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList()))
      .filter(causeList -> paramNameIterator.hasNext())
      .forEach(causeList -> {
        List<String> parameters = paramNameIterator.next();
        Assert.assertEquals(parameters.size(), causeList.size());
        IntStream.range(0, parameters.size()).forEach(i -> {
          ValidationFailure.Cause cause = causeList.get(i);
          Assert.assertEquals(parameters.get(i), cause.getAttribute(CauseAttributes.STAGE_CONFIG));
        });
      });
  }
}
