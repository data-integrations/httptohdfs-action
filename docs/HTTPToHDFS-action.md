# HTTP To HDFS Action


Description
-----------
Action to fetch data from an external http endpoint and create a file in HDFS.

Properties
----------

**hdfsFilePath:** The location to write the data in HDFS. If the file already exists, it will be overwritten.

**url:** The URL to fetch data from.

**method:** The HTTP request method. GET and POST are the allowed methods.

**body:** Optional request body.

**outputFormat:** Output data should be written as Text (JSON, XML, txt files) or Binary (zip, gzip, images). Defaults to Text.

**charset:** If text data is selected, this should be the charset of the text being returned. Defaults to UTF-8.

**requestHeaders:** An optional string of header values to send in each request where the keys and values are
delimited by a colon (":") and each pair is delimited by a newline ("\n").

**followRedirects:** Whether to automatically follow redirects. Defaults to true.

**numRetries:** The number of times the request should be retried if the request fails. Defaults to 3.

**connectTimeout:** The time in milliseconds to wait for a connection. Set to 0 for infinite. Defaults to 60000 (1 minute).

**readTimeout:** The time in milliseconds to wait for a read. Set to 0 for infinite. Defaults to 60000 (1 minute).

**disableSSLValidation:**  If user enables SSL validation, they will be expected to add the certificate to the trustStore on each machine. Defaults to true.

**outputPath:** The key used to store the file path for the data that was written so that the file source can read from it.
                Plugins that run at later stages in the pipeline can retrieve the file path using this key through macro
                substitution:${filePath} where "filePath" is the key specified. Defaults to "filePath".

**responseHeaders:** The key used to store the response headers so that they are available to other plugins down the line.
                     Plugins that run at later stages in the pipeline can retrieve the response headers using this through macro substitution:${responseHeaders}
                     where "responseHeaders" is the key specified. "Defaults to "responseHeaders".


Example
-------
This example performs HTTP GET request to http://example.com/data and downloads the csv file to file:///tmp/data.csv.

    {
        "name": "HTTPToHDFSAction",
        "type": "action",
        "properties": {
            "hdfsFilePath": "file://tmp/data.csv",
            "url": "http://example.com/data",
            "method": "GET",
            "outputFormat": "Text",
            "charset": "UTF-8",
            "followRedirects": "true",
            "disableSSLValidation": "true",
            "numRetries": 0,
            "connectTimeout": 60000,
            "readTimeout": 60000
        }
    }
