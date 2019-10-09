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
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.HttpMethod;

/**
 * Config for the http to hdfs action.
 */
public class HTTPToHDFSActionConfig extends PluginConfig {

  private static final Set<String> METHODS = ImmutableSet.of(HttpMethod.GET, HttpMethod.POST);
  private static final String KV_DELIMITER = ":";
  private static final String DELIMITER = "\n";

  public static final String URL = "url";
  public static final String METHOD = "method";
  public static final String REQUEST_HEADERS = "requestHeaders";
  public static final String NUM_RETRIES = "numRetries";
  public static final String CONNECT_TIMEOUT = "connectTimeout";
  public static final String READ_TIMEOUT = "readTimeout";

  @Description("The location to write the data in HDFS. If the file already exists, it will be overwritten.")
  @Macro
  private String hdfsFilePath;

  @Name(URL)
  @Description("The URL to fetch data from.")
  @Macro
  private String url;

  @Name(METHOD)
  @Description("The http request method.")
  private String method;

  @Nullable
  @Description("The http request body.")
  @Macro
  private String body;

  @Name(REQUEST_HEADERS)
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

  @Name(NUM_RETRIES)
  @Description("The number of times the request should be retried if the request fails. Defaults to 3.")
  private Integer numRetries;

  @Name(CONNECT_TIMEOUT)
  @Description("Sets the connection timeout in milliseconds. Set to 0 for infinite. Default is 60000 (1 minute).")
  @Nullable
  @Macro
  private Integer connectTimeout;

  @Name(READ_TIMEOUT)
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

  private HTTPToHDFSActionConfig(Builder builder) {
    hdfsFilePath = builder.hdfsFilePath;
    url = builder.url;
    requestHeaders = builder.requestHeaders;
    outputFormat = builder.outputFormat;
    charset = builder.charset;
    method = builder.method;
    body = builder.body;
    numRetries = builder.numRetries;
    readTimeout = builder.readTimeout;
    connectTimeout = builder.connectTimeout;
    followRedirects = builder.followRedirects;
    disableSSLValidation = builder.disableSSLValidation;
    outputPath = builder.outputPath;
    responseHeaders = builder.responseHeaders;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(HTTPToHDFSActionConfig copy) {
    return builder()
      .setHdfsFilePath(copy.hdfsFilePath)
      .setUrl(copy.url)
      .setRequestHeaders(copy.requestHeaders)
      .setOutputFormat(copy.outputFormat)
      .setCharset(copy.charset)
      .setMethod(copy.method)
      .setBody(copy.body)
      .setNumRetries(copy.numRetries)
      .setReadTimeout(copy.readTimeout)
      .setConnectTimeout(copy.connectTimeout)
      .setFollowRedirects(copy.getFollowRedirects())
      .setDisableSSLValidation(copy.disableSSLValidation)
      .setOutputPath(copy.outputPath)
      .setResponseHeaders(copy.responseHeaders);
  }

  public String getHdfsFilePath() {
    return hdfsFilePath;
  }

  public String getUrl() {
    return url;
  }

  public String getMethod() {
    return method;
  }

  @Nullable
  public String getBody() {
    return body;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public String getCharset() {
    return charset;
  }

  public Boolean getFollowRedirects() {
    return followRedirects;
  }

  public Boolean getDisableSSLValidation() {
    return disableSSLValidation;
  }

  public Integer getNumRetries() {
    return numRetries;
  }

  @Nullable
  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  @Nullable
  public Integer getReadTimeout() {
    return readTimeout;
  }

  @Nullable
  public String getOutputPath() {
    return outputPath;
  }

  @Nullable
  public String getResponseHeaders() {
    return responseHeaders;
  }

  public Map<String, String> getRequestHeadersMap() {
    return convertHeadersToMap(requestHeaders);
  }

  public void validate(FailureCollector failureCollector) {
    try {
      new URL(url);
    } catch (MalformedURLException e) {
      failureCollector.addFailure(String.format("URL '%s' is malformed: '%s'", url, e.getMessage()), null)
        .withConfigProperty(URL);
    }
    if (!containsMacro(CONNECT_TIMEOUT) && connectTimeout != null && connectTimeout < 0) {
      failureCollector.addFailure(String.format("Invalid connection timeout '%d'.", connectTimeout),
                                  "Connection timeout must be 0 or a positive number.")
        .withConfigProperty(CONNECT_TIMEOUT);
    }
    if (!containsMacro(REQUEST_HEADERS) && !Strings.isNullOrEmpty(requestHeaders)) {
      for (String chunk : requestHeaders.split(DELIMITER)) {
        String[] keyValue = chunk.split(KV_DELIMITER);
        if (keyValue.length != 2) {
          failureCollector.addFailure(String.format("Unable to parse key-value pair '%s'.", chunk),
                                      "Provide correct value for request headers field.")
            .withConfigProperty(REQUEST_HEADERS);
        }
      }
    }
    if (!METHODS.contains(method.toUpperCase())) {
      failureCollector.addFailure(
        String.format("Invalid request method '%s'.", method),
        String.format("Request method must be one of '%s'.", Joiner.on(',').join(METHODS)))
        .withConfigProperty(METHOD);
    }
    if (!containsMacro(READ_TIMEOUT) && readTimeout != null && readTimeout < 0) {
      failureCollector.addFailure(
        String.format("Invalid read timeout '%d'.", readTimeout),
        "Read timeout must be 0 or a positive number.")
        .withConfigProperty(READ_TIMEOUT);
    }
  }

  private Map<String, String> convertHeadersToMap(String headersString) {
    Map<String, String> headersMap = new HashMap<>();
    if (!Strings.isNullOrEmpty(headersString)) {
      for (String chunk : headersString.split(DELIMITER)) {
        String[] keyValue = chunk.split(KV_DELIMITER, 2);
        if (keyValue.length == 2) {
          headersMap.put(keyValue[0], keyValue[1]);
        }
      }
    }
    return headersMap;
  }

  /**
   * Builder for HTTPToHDFSActionConfig
   */
  public static final class Builder {
    private String hdfsFilePath;
    private String url;
    private String method;
    private String body;
    private String requestHeaders;
    private String outputFormat;
    private String charset;
    private Boolean followRedirects;
    private Boolean disableSSLValidation;
    private Integer numRetries;
    private Integer connectTimeout;
    private Integer readTimeout;
    private String outputPath;
    private String responseHeaders;

    private Builder() {
    }

    public Builder setHdfsFilePath(String hdfsFilePath) {
      this.hdfsFilePath = hdfsFilePath;
      return this;
    }

    public Builder setUrl(String url) {
      this.url = url;
      return this;
    }

    public Builder setMethod(String method) {
      this.method = method;
      return this;
    }

    public Builder setBody(String body) {
      this.body = body;
      return this;
    }

    public Builder setRequestHeaders(String requestHeaders) {
      this.requestHeaders = requestHeaders;
      return this;
    }

    public Builder setOutputFormat(String outputFormat) {
      this.outputFormat = outputFormat;
      return this;
    }

    public Builder setCharset(String charset) {
      this.charset = charset;
      return this;
    }

    public Builder setFollowRedirects(Boolean followRedirects) {
      this.followRedirects = followRedirects;
      return this;
    }

    public Builder setDisableSSLValidation(Boolean disableSSLValidation) {
      this.disableSSLValidation = disableSSLValidation;
      return this;
    }

    public Builder setNumRetries(Integer numRetries) {
      this.numRetries = numRetries;
      return this;
    }

    public Builder setConnectTimeout(Integer connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder setReadTimeout(Integer readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    public Builder setOutputPath(String outputPath) {
      this.outputPath = outputPath;
      return this;
    }

    public Builder setResponseHeaders(String responseHeaders) {
      this.responseHeaders = responseHeaders;
      return this;
    }

    public HTTPToHDFSActionConfig build() {
      return new HTTPToHDFSActionConfig(this);
    }
  }
}
