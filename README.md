# ultra-csv

A smart reader for CSV files.

## Features

  - Smart statistical heuristics to guess pretty much anything about
  your csv file, from delimiter to quotes and whether a header is present
  - Handles for you the boring but dangerous stuff, like encoding detection
  and bom skipping if present, but also embedded new lines and quote escaping
  - Uses [Prismatic Schema][https://github.com/Prismatic/schema] to coerce the numerical
  values that have been recognised. The types and coercions can be extended by the user to *dates*,
  *phone numbers*, etc. and the inputs can be validated
  - Designed to be both very easy to use in an exploratory way to get a quick feel for the
  data, and then be put into production with almost the same code
  - Special tools to use the parsing features of *ultra-csv* inside environments
  where you access the data line by line as Strings, like *Hadoop*

## Installation

`ultra-csv` is available as a Maven artifact from
[Clojars](http://clojars.org/ultra-csv):

In your `project.clj` dependencies for *leiningen*:

```clojure
[ultra-csv "0.2.0"]
```

## Usage

```clojure
(def csv-seq (ultra-csv/read-csv "/my/path/to.csv"))
```

## TODO

  - Add writer functionality with roud-tripping capability
  - Add some kind of integration with [Incanter][http://incanter.org/] to easily produce Datasets
  - If possible, add more Type detections (dates perhaps?)

Pull Requests welcome!

## License

Copyright © 2014 Nils Grunwald

Distributed under the Eclipse Public License, the same as Clojure.
