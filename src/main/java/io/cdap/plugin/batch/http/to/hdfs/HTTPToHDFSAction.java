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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Action to fetch data from an external http endpoint and create a file in HDFS.
 */

@Plugin(type = Action.PLUGIN_TYPE)
@Name("HTTPToHDFS")
@Description("Action to fetch data from an external http endpoint and create a file in HDFS.")
public class HTTPToHDFSAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(HTTPToHDFSAction.class);
  private static final int BUFFER_SIZE = 4096;

  private final HTTPToHDFSActionConfig config;

  public HTTPToHDFSAction(HTTPToHDFSActionConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    int retries = 0;
    Exception exception = null;
    do {
      HttpURLConnection conn = null;
      Map<String, String> headers = config.getRequestHeadersMap();
      try {
        URL url = new URL(config.getUrl());
        conn = (HttpURLConnection) url.openConnection();
        if (conn instanceof HttpsURLConnection) {
          //Disable SSLv3
          System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
          if (config.getDisableSSLValidation()) {
            disableSSLValidation();
          }
        }
        conn.setRequestMethod(config.getMethod().toUpperCase());
        conn.setConnectTimeout(config.getConnectTimeout());
        conn.setReadTimeout(config.getReadTimeout());
        conn.setInstanceFollowRedirects(config.getFollowRedirects());
        for (Map.Entry<String, String> propertyEntry : headers.entrySet()) {
          conn.addRequestProperty(propertyEntry.getKey(), propertyEntry.getValue());
        }
        if (config.getBody() != null) {
          conn.setDoOutput(true);
          try (OutputStream outputStream = conn.getOutputStream()) {
            outputStream.write(config.getBody().getBytes(config.getCharset()));
          }
        }
        Path file = new Path(config.getHdfsFilePath());
        try (FileSystem fs = FileSystem.get(file.toUri(), new Configuration());
             FSDataOutputStream outputStream = fs.create(file, true);
             InputStream inputStream = conn.getInputStream()
        ) {
          if (inputStream != null) {
            if (config.getOutputFormat().equalsIgnoreCase("Binary")) {
              int i = 0;
              byte[] bytesIn = new byte[BUFFER_SIZE];
              while ((i = inputStream.read(bytesIn)) >= 0) {
                outputStream.write(bytesIn, 0, i);
              }
            } else if (config.getOutputFormat().equalsIgnoreCase("Text")) {
              try (BufferedReader bufferedReader = new BufferedReader
                (new InputStreamReader(inputStream, config.getCharset()));) {
                int i = 0;
                char charsIn[] = new char[BUFFER_SIZE];
                while ((i = bufferedReader.read(charsIn)) >= 0) {
                  outputStream.write(new String(charsIn).getBytes(), 0, i);
                }
              }
            }
          }
          context.getArguments().set(config.getOutputPath(), config.getHdfsFilePath());
          Map<String, String> flattenedHeaders = new HashMap<>();
          for (Map.Entry<String, List<String>> k : conn.getHeaderFields().entrySet()) {
            if (!Strings.isNullOrEmpty(k.getKey())) {
              flattenedHeaders.put(k.getKey(), Joiner.on(',').skipNulls().join(k.getValue()));
            }
          }
          context.getArguments().set(config.getResponseHeaders(), new Gson().toJson(flattenedHeaders));
        }
        break;
      } catch (MalformedURLException | ProtocolException e) {
        throw new IllegalStateException("Error opening url connection. Reason: " + e.getMessage(), e);
      } catch (Exception e) {
        LOG.warn("Error making {} request to url {} with headers {}.", config.getMethod(), config.getUrl(), headers);
        exception = e;
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
      retries++;
    } while (retries < config.getNumRetries());
    if (exception != null) {
      throw exception;
    }
  }

  private void disableSSLValidation() {
    TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
      public X509Certificate[] getAcceptedIssuers() {
        return null;
      }

      public void checkClientTrusted(X509Certificate[] certs, String authType) {
      }

      public void checkServerTrusted(X509Certificate[] certs, String authType) {
      }
    }
    };
    SSLContext sslContext = null;
    try {
      sslContext = SSLContext.getInstance("SSL");
      sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
    } catch (KeyManagementException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Error while installing the trust manager: " + e.getMessage(), e);
    }
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
    HostnameVerifier allHostsValid = new HostnameVerifier() {
      public boolean verify(String hostname, SSLSession session) {
        return true;
      }
    };
    HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
  }
}
