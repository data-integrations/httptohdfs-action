# HTTP To HDFS Action


Description
-----------
Action to fetch data from an external http webservice or url and dump the data into HDFS.

Use Cases
--------
1.I would like to store consumer complaints data from http://www.consumerfinance.gov/data-research/consumer-complaints/#download-the-data into my cluster.
The data is updated nightly, and is 260mb, so i would like to build a pipeline to run every 24 hours and refresh the data from the site.
Using this plugin, i configure it to pull data from the url in csv format, and store it in hdfs.
Then i configure a File source, a CSV parser, and a table sink to process the data in hydrator.
2.The inventory team publishes a product inventory xml feed that contains all the current products and the quantity of the item.
I would like to configure a pipeline to query that service every hour, and write the data into a table.
Using this plugin, i configure it to request information from the url, provide my credentials and expected format as request headers,
and write the results to the /tmp/ dir in HDFS so that the rest of the pipeline can process the data.
3.I have partnered with a 3rd party company that is sending me a large amount of data in a gzip file.
The file is stored on a webserver somewhere and i am given a url to download it.
Using this plugin, i configure it to pull the binary file, store the .gz file on hdfs, and use the file source to natively read that data for processing.


Properties
----------

**hdfsFilePath:** The location to write the data in HDFS. If the file already exists,same will be overwritten.

**url:** The URL to fetch data from.

**method:** The HTTP request method.GET and POST are the allowed methods.

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
            "readTimeout": 60000,
            "format" : "csv"
        }
    }
