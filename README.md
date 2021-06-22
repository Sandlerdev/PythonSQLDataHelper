# PythonSQLDataHelper
Small Class that simplifies fetching data from SQL Table configured for Narrow Time Series data.  The class has a method to pivot the data to a wide format and offers some simple caching of requests to help with performance.  

- This should work as is for SQL DB created from FactoryTalk Edge Gateway.
- Will also work with the default database created from this solution [Blob Parser](https://github.com/Sandlerdev/BlobParser)
