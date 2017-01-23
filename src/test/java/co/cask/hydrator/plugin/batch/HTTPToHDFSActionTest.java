/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import co.cask.hydrator.plugin.mock.MockFeedHandler;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.HttpMethod;

/**
 * Test for HTTP to HDFS Action
 */
public class HTTPToHDFSActionTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId BATCH_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "4.0.0");
  protected static final ArtifactSummary BATCH_ARTIFACT = new ArtifactSummary("data-pipeline", "4.0.0");

  private static NettyHttpService httpService;
  protected static String baseURL;

  @BeforeClass
  public static void setupTestClass() throws Exception {

    setupBatchArtifacts(BATCH_ARTIFACT_ID, DataPipelineApp.class);
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(NamespaceId.DEFAULT, BATCH_ARTIFACT_ID.getArtifact(),
                                  new ArtifactVersion(BATCH_ARTIFACT.getVersion()), true,
                                  new ArtifactVersion(BATCH_ARTIFACT.getVersion()), true));
    addPluginArtifact(NamespaceId.DEFAULT.artifact("httptohdfs-action-plugin", "1.6.0"), parents,
                      HTTPToHDFSAction.class);

    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new MockFeedHandler());
    httpService = NettyHttpService.builder("MockService").addHttpHandlers(handlers).build();
    httpService.startAndWait();

    int port = httpService.getBindAddress().getPort();
    baseURL = "http://localhost:" + port;
    // tell service what its port is.
    URL setPortURL = new URL(baseURL + "/feeds/users");
    HttpURLConnection urlConn = (HttpURLConnection) setPortURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod(HttpMethod.PUT);
    urlConn.getOutputStream().write("samuel jackson, dwayne johnson, christopher walken".getBytes(Charsets.UTF_8));
    Assert.assertEquals(200, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  @AfterClass
  public static void teardown() {
    httpService.stopAndWait();
  }

  @After
  public void cleanupTest() throws IOException {
    resetFeeds();
  }

  @Test
  public void testHTTPToHDFSAction() throws Exception {

    String filePath = "/resources/data.txt";
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("url", baseURL + "/feeds/users/")
      .put("method", "GET")
      .put("outputFormat", "Text")
      .put("charset", "UTF-8")
      .put("hdfsFilePath", filePath)
      .put("numRetries", "0")
      .put("followRedirects", "true")
      .put("disableSSLValidation", "true")
      .build();

    ETLStage action = new ETLStage("http", new ETLPlugin("HTTPToHDFS", Action.PLUGIN_TYPE, properties, null));
    ETLStage source = new ETLStage("source", MockSource.getPlugin("httpCallbackInput"));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin("httpCallbackOutput"));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(action)
      .addConnection(source.getName(), sink.getName())
      .addConnection(sink.getName(), action.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("httptohdfsactionTest");
    ApplicationManager appManager = TestBase.deployApplication(appId, appRequest);

    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.start();
    manager.waitForFinish(5, TimeUnit.MINUTES);
    String expectedOutput = "samuel jackson, dwayne johnson, christopher walken";
    String output;
    try (FileInputStream inputStream = new FileInputStream(filePath)) {
      output = IOUtils.toString(inputStream);
    }
    Assert.assertEquals(expectedOutput, output);
  }

  private int resetFeeds() throws IOException {
    URL url = new URL(baseURL + "/feeds");
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod(HttpMethod.DELETE);
    int responseCode = urlConn.getResponseCode();
    urlConn.disconnect();
    return responseCode;
  }
}
