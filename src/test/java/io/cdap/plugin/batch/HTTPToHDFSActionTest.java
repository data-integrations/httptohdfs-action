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

package io.cdap.plugin.batch;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.TestBase;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import io.cdap.plugin.mock.MockFeedHandler;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
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

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  protected static final ArtifactId BATCH_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "4.0.0");
  protected static final ArtifactSummary BATCH_ARTIFACT = new ArtifactSummary("data-pipeline", "4.0.0");
  protected static String baseURL;

  private static NettyHttpService httpService;
  private static File resourceFolder;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    setupBatchArtifacts(BATCH_ARTIFACT_ID, DataPipelineApp.class);
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), BATCH_ARTIFACT_ID.getArtifact(),
                                  new ArtifactVersion(BATCH_ARTIFACT.getVersion()), true,
                                  new ArtifactVersion(BATCH_ARTIFACT.getVersion()), true));
    addPluginArtifact(NamespaceId.DEFAULT.artifact("httptohdfs-action-plugin", "1.6.0"), parents,
                      HTTPToHDFSAction.class);

    resourceFolder = temporaryFolder.newFolder("resource");

    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new MockFeedHandler());
    httpService = NettyHttpService.builder("MockService").setHttpHandlers(handlers).build();
    httpService.start();

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
  public static void teardown() throws Exception {
    httpService.stop();
  }

  @After
  public void cleanupTest() throws IOException {
    resetFeeds();
  }

  @Test
  public void testHTTPToHDFSAction() throws Exception {
    File dataFile = new File(resourceFolder, "data.txt");
    String filePath = dataFile.getAbsolutePath();
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
    manager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);
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
