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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
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
import java.util.Set;
import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.HttpMethod;

/**
 * Action to fetch data from an external http endpoint and create a file in HDFS.
 */

@Plugin(type = Action.PLUGIN_TYPE)
@Name("HTTPToHDFS")
@Description("Action to fetch data from an external http endpoint and create a file in HDFS.")
public class HTTPToHDFSAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(HTTPToHDFSAction.class);
  private static final String KV_DELIMITER = ":";
  private static final String DELIMITER = "\n";
  private static final int BUFFER_SIZE = 4096;
  private static final Set<String> METHODS = ImmutableSet.of(HttpMethod.GET, HttpMethod.POST);

  private final HTTPToHDFSActionConfig config;

  public HTTPToHDFSAction(HTTPToHDFSActionConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate();
    int retries = 0;
    Exception exception = null;
    do {
      HttpURLConnection conn = null;
      Map<String, String> headers = config.getRequestHeadersMap();
      try {
        URL url = new URL(config.url);
        conn = (HttpURLConnection) url.openConnection();
        if (conn instanceof HttpsURLConnection) {
          //Disable SSLv3
          System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
          if (config.disableSSLValidation) {
            disableSSLValidation();
          }
        }
        conn.setRequestMethod(config.method.toUpperCase());
        conn.setConnectTimeout(config.connectTimeout);
        conn.setReadTimeout(config.readTimeout);
        conn.setInstanceFollowRedirects(config.followRedirects);
        for (Map.Entry<String, String> propertyEntry : headers.entrySet()) {
          conn.addRequestProperty(propertyEntry.getKey(), propertyEntry.getValue());
        }
        if (config.body != null) {
          conn.setDoOutput(true);
          try (OutputStream outputStream = conn.getOutputStream()) {
            outputStream.write(config.body.getBytes(config.charset));
          }
        }
        Path file = new Path(config.hdfsFilePath);
        try (FileSystem fs = FileSystem.get(file.toUri(), new Configuration());
             FSDataOutputStream outputStream = fs.create(file, true);
             InputStream inputStream = conn.getInputStream()
        ) {
          if (inputStream != null) {
            if (config.outputFormat.equalsIgnoreCase("Binary")) {
              int i = 0;
              byte[] bytesIn = new byte[BUFFER_SIZE];
              while ((i = inputStream.read(bytesIn)) >= 0) {
                outputStream.write(bytesIn, 0, i);
              }
            } else if (config.outputFormat.equalsIgnoreCase("Text")) {
              try (BufferedReader bufferedReader = new BufferedReader
                (new InputStreamReader(inputStream, config.charset));) {
                int i = 0;
                char charsIn[] = new char[BUFFER_SIZE];
                while ((i = bufferedReader.read(charsIn)) >= 0) {
                  outputStream.write(new String(charsIn).getBytes(), 0, i);
                }
              }
            }
          }
          context.getArguments().set(config.outputPath, config.hdfsFilePath);
          Map<String, String> flattenedHeaders = new HashMap<>();
          for (Map.Entry<String, List<String>> k : conn.getHeaderFields().entrySet()) {
            if (!Strings.isNullOrEmpty(k.getKey())) {
              flattenedHeaders.put(k.getKey(), Joiner.on(',').skipNulls().join(k.getValue()));
            }
          }
          context.getArguments().set(config.responseHeaders, new Gson().toJson(flattenedHeaders));
        }
        break;
      } catch (MalformedURLException | ProtocolException e) {
        throw new IllegalStateException("Error opening url connection. Reason: " + e.getMessage(), e);
      } catch (Exception e) {
        LOG.warn("Error making {} request to url {} with headers {}.", config.method, config.url, headers);
        exception = e;
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
      retries++;
    } while (retries < config.numRetries);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
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

  /**
   * Config for the http to hdfs action.
   */
  public static class HTTPToHDFSActionConfig extends PluginConfig {

    @Description("The location to write the data in HDFS. If the file already exists, it will be overwritten.")
    @Macro
    private String hdfsFilePath;

    @Description("The URL to fetch data from.")
    @Macro
    private String url;

    @Description("The http request method.")
    private String method;

    @Nullable
    @Description("The http request body.")
    @Macro
    private String body;

    @Nullable
    @Description("Request headers to set when performing the http request.")
    @Macro
    private String requestHeaders;

    @Description("Output data should be written as Text (JSON, XML, txt files) or Binary (zip, gzip, images). " +
      "Defaults to Text.")
    private String outputFormat;

    @Description("If text data is selected, this should be the charset of the text being returned. Defaults to UTF-8.")
    private String charset;

    @Description("Whether to automatically follow redirects. Defaults to true.")
    private Boolean followRedirects;

    @Description("If user enables SSL validation, they will be expected to add the certificate to the trustStore" +
      " on each machine. Defaults to true.")
    private Boolean disableSSLValidation;

    @Description("The number of times the request should be retried if the request fails. Defaults to 3.")
    private Integer numRetries;

    @Description("Sets the connection timeout in milliseconds. Set to 0 for infinite. Default is 60000 (1 minute).")
    @Nullable
    @Macro
    private Integer connectTimeout;

    @Description("The time in milliseconds to wait for a read. Set to 0 for infinite. Defaults to 60000 (1 minute).")
    @Nullable
    @Macro
    private Integer readTimeout;

    @Nullable
    @Description("The key used to store the file path for the data that was written so that the file source can read " +
      "from it. Plugins that run at later stages in the pipeline can retrieve the file path using this key " +
      "through macro substitution:${filePath} where \"filePath\" is the key specified. Defaults to \"filePath\".")
    @Macro
    private String outputPath;

    @Nullable
    @Description("The key used to store the response headers so that they are available to other plugins " +
      "down the line. Plugins that run at later stages in the pipeline can retrieve the response headers using this " +
      "through macro substitution:${responseHeaders} where \"responseHeaders\" is the key specified. " +
      "Defaults to \"responseHeaders\".")
    @Macro
    private String responseHeaders;

    public HTTPToHDFSActionConfig() {
      //Default values are set
      this.connectTimeout = 60 * 1000;
      this.readTimeout = 60 * 1000;
      this.numRetries = 3;
      this.followRedirects = true;
      this.disableSSLValidation = true;
      this.charset = "UTF-8";
      this.outputFormat = "Text";
      this.outputPath = "filePath";
      this.responseHeaders = "responseHeaders";
      this.method = "GET";
    }

    public HTTPToHDFSActionConfig(String hdfsFilePath, String url, String method, @Nullable String body,
                                  @Nullable String requestHeaders, String outputFormat, String charset,
                                  boolean followRedirects, boolean disableSSLValidation, @Nullable int numRetries,
                                  @Nullable int readTimeout, @Nullable int connectTimeout,
                                  @Nullable String outputPath, @Nullable String responseHeaders) {

      this.hdfsFilePath = hdfsFilePath;
      this.url = url;
      this.requestHeaders = requestHeaders;
      this.outputFormat = outputFormat;
      this.charset = charset;
      this.method = method;
      this.body = body;
      this.numRetries = numRetries;
      this.readTimeout = readTimeout;
      this.connectTimeout = connectTimeout;
      this.followRedirects = followRedirects;
      this.disableSSLValidation = disableSSLValidation;
      this.outputPath = outputPath;
      this.responseHeaders = responseHeaders;
    }

    public Map<String, String> getRequestHeadersMap() {
      return convertHeadersToMap(requestHeaders);
    }

    public void validate() {
      try {
        new URL(url);
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(String.format("URL '%s' is malformed: %s", url, e.getMessage()), e);
      }
      if (!containsMacro("connectTimeout") && connectTimeout < 0) {
        throw new IllegalArgumentException(String.format(
          "Invalid connectTimeout %d. Timeout must be 0 or a positive number.", connectTimeout));
      }
      convertHeadersToMap(requestHeaders);
      if (!containsMacro("method") && !METHODS.contains(method.toUpperCase())) {
        throw new IllegalArgumentException(String.format("Invalid request method %s, must be one of %s.",
                                                         method, Joiner.on(',').join(METHODS)));
      }
      if (!containsMacro("numRetries") && numRetries < 0) {
        throw new IllegalArgumentException(String.format(
          "Invalid numRetries %d. Retries cannot be a negative number.", numRetries));
      }
      if (!containsMacro("readTimeout") && readTimeout < 0) {
        throw new IllegalArgumentException(String.format(
          "Invalid readTimeout %d. Timeout must be 0 or a positive number.", readTimeout));
      }
    }

    private Map<String, String> convertHeadersToMap(String headersString) {
      Map<String, String> headersMap = new HashMap<>();
      if (!Strings.isNullOrEmpty(headersString)) {
        for (String chunk : headersString.split(DELIMITER)) {
          String[] keyValue = chunk.split(KV_DELIMITER, 2);
          if (keyValue.length == 2) {
            headersMap.put(keyValue[0], keyValue[1]);
          } else {
            throw new IllegalArgumentException(String.format("Unable to parse key-value pair '%s'.", chunk));
          }
        }
      }
      return headersMap;
    }
  }
}
