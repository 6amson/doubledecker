* parsed the csv field to lower case when the datafusion describe method threw a mismatch error.
* moved form nested loops for extracting rows and parsing their values to Arraywriter
* used arrow-json write builder to ensure this parsing includes columsn with null values LineDelimitedWriter and ArrayWriter will omit writing keys with null values.
In order to explicitly write null values for keys, configure a custom Writer by
using a WriterBuilder to construct a Writer.
§Writing to serde_json JSON Objects
To serialize RecordBatches into an array of
JSON objects you can reparse the resulting JSON string.
Note that this is less efficient than using the Writer API.