(ns ultra-csv.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure
             [walk :as walk]
             [data :as data]]
            [schema [core :as s
                     :refer [maybe Int Keyword optional-key required-key
                             one optional]]
             [coerce :as c]]
            [clojure.tools.logging :as log])
  (:import [org.supercsv.io
            CsvMapReader CsvListReader AbstractCsvReader
            CsvMapWriter CsvListWriter AbstractCsvWriter]
           [org.supercsv.prefs CsvPreference CsvPreference$Builder]
           [java.io
            Reader BufferedReader InputStream PushbackInputStream
            StringWriter Writer File]
           [java.nio.charset Charset]
           [com.ibm.icu.text CharsetDetector]
           [java.text SimpleDateFormat]))

(defn write-spec
  "Writes a spec struct to a String"
  [spec]
  (pr-str spec))

(defn load-spec-str
  "Loads a spec structure from String form. **BEWARE** The uses eval
   on the passed String, only use on inputs that are **ABSOLUTELY SAFE**"
  [spec-string]
  (binding [*ns* (the-ns 'ultra-csv.core)]
    (in-ns 'ultra-csv.core)
    (load-string spec-string)))

(declare analyze-csv)

(def ^:no-doc available-charsets (into #{} (.keySet (Charset/availableCharsets))))

(defn ^:no-doc guess-charset
  [^InputStream is]
  (try
    (let [^CharsetDetector detector (CharsetDetector.)]
      (.enableInputFilter detector true)
      (.setText detector is)
      (let [m (.detect detector)
            encoding (.getName m)]
        (if (available-charsets encoding)
          encoding
          "utf-8")))
    (catch Exception e "utf-8")))

(def ^:no-doc boms
  {[(byte -1) (byte -2)] [:utf16-le 2]
   [(byte -2) (byte -1)] [:utf16-be 2]
   [(byte -17) (byte -69) (byte -65)] [:utf8 3]
   [(byte -1) (byte -2) (byte 0) (byte 0)] [:utf32-le 4]
   [(byte 0) (byte 0) (byte -2) (byte -1)] [:utf32-be 4]
   [] [:none 0]})

(def ^:no-doc bom-sizes
  (reduce (fn
            [acc [_ [bom-name bom-size]]]
            (assoc acc bom-name bom-size))
          {} boms))

(def ^:no-doc bom-bytes
  (reduce (fn
            [acc [bts [bom-name _]]]
            (assoc acc bom-name bts))
          {} boms))

(defn ^:no-doc skip-bom-from-stream-if-present
  [stream]
  (let [pbis (PushbackInputStream. stream 4)
        bom (byte-array 4)]
    (.read pbis bom)
    (let [[a b c d :as first-four] (into [] (seq bom))
          first-two [a b]
          first-three [a b c]
          [bom-name bom-size] (or (boms first-two) (boms first-three) (boms first-four))]
      (if bom-size
        (let [to-put-back (byte-array (drop bom-size bom))]
          (.unread pbis to-put-back)
          [(io/input-stream pbis) bom-name])
        (do
          (.unread pbis bom)
          [(io/input-stream pbis) :none])))))

(defn ^:no-doc get-reader
  ([src encoding bom]
     (if (instance? java.io.Reader src)
       [src (fn [& _] nil) encoding nil]
       (let [[raw-stream close-fn] (if (instance? java.io.InputStream src)
                                     [src (fn [& _] nil)]
                                     (let [istream (io/input-stream src)]
                                       [istream
                                        (fn [& args]
                                          (.close istream)
                                          (doseq [arg args]
                                            (.close arg))
                                          true)]))
             enc (or encoding (guess-charset raw-stream))
             [^InputStream istream bom-name] (if (nil? bom)
                                               (skip-bom-from-stream-if-present raw-stream)
                                               (do (.read raw-stream (byte-array (get bom-sizes bom 0)))
                                                   [raw-stream bom]))
             rdr (io/reader istream :encoding enc)]
         [rdr close-fn enc bom-name])))
  ([src encoding] (get-reader src encoding nil))
  ([src] (get-reader src nil nil)))

(defn ^:no-doc read-row
  [^AbstractCsvReader rdr read-from-csv transform-line clean-rdr limit]
  (let [res (read-from-csv rdr)]
    (if (and res (or (nil? limit) (< (.getLineNumber rdr) limit)))
      (cons
       (vary-meta (transform-line res) assoc ::csv-reader rdr ::clean-reader clean-rdr)
       (lazy-seq (read-row rdr read-from-csv transform-line clean-rdr limit)))
      (clean-rdr rdr))))

(defn make-date-coercer
  "Makes a date coercer to use with the *coercer* option of [[read-csv]].
Given a formatter *String* and an optional timezonr *String*, this will return
a ready to use function. Details on the format can be found at
http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html"
  ([fmt]
     (let [formatter (SimpleDateFormat. fmt)]
       (fn [^String s] (.parse formatter s))))
  ([fmt timezone]
     (let [formatter (SimpleDateFormat. fmt)]
       (.setTimeZone formatter (java.util.TimeZone/getTimeZone timezone))
       (fn [^String s] (.parse formatter s)))))

(defn ^:no-doc greedy-read-fn
  [^AbstractCsvReader rdr read-from-csv transform-line clean-rdr limit]
  (->
   (fn []
     (let [res (read-from-csv rdr)]
       (when (and res (or (nil? limit) (< (.getLineNumber rdr) limit)))
         (transform-line res))))
   (vary-meta assoc ::csv-reader rdr ::clean-reader clean-rdr)))

(defn ^:no-doc line-read-fn
  [read-from-csv transform-line]
  (fn [src]
    (let [[rdr close-rdr] (if (instance? java.io.Reader src)
                            [src (fn [] nil)]
                            (let [rdr (java.io.StringReader. src)]
                              [rdr (fn [] (.close rdr))]))]
      (let [entity (when-let [res (read-from-csv rdr)]
                     (transform-line res))]
        (close-rdr)
        entity))))

(defn ^:no-doc find-char-pos
  [^String line char]
  (loop [found []
         cur 0]
    (let [pos (.indexOf line (int char) cur)]
      (if (not= pos -1)
        (recur (conj found pos) (inc pos))
        found))))

(defn ^:no-doc guess-delimiter
  [lines]
  (let [all-dels (for [line lines
                       :let [clean-line (str/replace line #"\"[^\"]*\"" "")]]
                   (into {}
                         (map
                          (fn [character]
                            [character
                             (count (find-char-pos clean-line character))])
                          [\, \; \space \tab \|])))
        freqs (first all-dels)
        report (loop [todo all-dels
                      candidates (into {}
                                       (map (fn [k] [k 0]) (keys (first all-dels))))]
                 (if-let [dels (first todo)]
                   (let [diffs (filter
                                (fn [[k v]] (or (= v 0) (not= v (freqs k))))
                                dels)]
                     (recur (rest todo) (reduce (fn [acc k]
                                                  (update-in acc [k] #(if % (inc %) 1)))
                                                candidates (map first diffs))))
                   candidates))
        [[fc fv] [_ sv] & _] (sort-by (fn [[k v]] v) report)]
    (when (or (< fv sv) (nil? sv))
      fc)))

(defn ^:no-doc csv-prefs
  ^CsvPreference
  [{:keys [quote-symbol delimiter end-of-lines quoted?]
    :or {quote-symbol \"
         end-of-lines "\n"
         delimiter \,
         quoted? true}}]
  (let [quote (if quoted? quote-symbol (char (int 0)))]
    (->
     (CsvPreference$Builder. quote (int delimiter) end-of-lines)
     (.build))))

(defn ^:no-doc parse-fields
  [lines delimiter]
  (let [txt (str/join "\n" lines)
        prefs (csv-prefs {:delimiter delimiter})]
    (with-open [rdr (java.io.StringReader. txt)]
      (let [listr (CsvListReader. rdr prefs)
            seg-lines (loop [out []]
                        (if-let [fields (.read listr)]
                          (recur (conj out (into [] fields)))
                          out))]
        (.close listr)
        seg-lines))))

(defn ^:no-doc int-string?
  [s]
  (if (re-matches #"-?\d+" s)
    true false))

(defn ^:no-doc double-string?
  [s]
  (if (re-matches #"-?\d+([\.,]\d+)?" s)
    true false))

(def ^:no-doc Num java.lang.Double)

(def ^:no-doc known-types
  [[Int int-string?]
   [Num double-string?]])

(def ^:no-doc csv-coercer
  {Int [(fn [s] (Long/parseLong s)) str]
   Num [(fn [s] (Double/parseDouble s)) str]
   Keyword [(fn [s] (keyword s)) name]})

(defn ^:no-doc extract-coercer
  [io coercer]
  (reduce
   (fn [acc [k fmt-spec]]
     (let [[in out] (if (fn? fmt-spec)
                      [fmt-spec str]
                      fmt-spec)]
       (assoc acc k (case io
                      :input in
                      :output out))))
   {} coercer))

(defn ^:no-doc merge-coercers
  [io & coercers]
  (if (empty? coercers)
    {}
    (let [io-coercers (map (partial extract-coercer io) coercers)]
      (apply merge io-coercers))))

(defn ^:no-doc take-higher
  [cands]
  (if (empty? cands)
    s/Str
    (let [lookfor (into #{} cands)]
      (first (remove nil? (filter #(contains? lookfor %) (map first known-types)))))))

(defn ^:no-doc keywordize-keys
  [hm]
  (persistent!
   (reduce (fn [acc [k v]]
             (assoc! acc (keyword k) v))
           (transient {}) hm)))

(defn ^:no-doc guess-types
  [lines]
  (for [offset (range (count (first lines)))]
    (loop [todo lines
           candidates known-types
           not-nil 0]
      (if-let [line (first todo)]
        (let [field (nth line offset)
              valid (reduce (fn [acc [typ f]]
                              (if
                               (or (nil? field) (f field))
                               (assoc acc typ f)
                               acc))
                            {} candidates)
              new-nil (if (nil? field) not-nil (inc not-nil))]
          (recur (rest todo) valid new-nil))
        (if (> not-nil (* 0.5 (count lines)))
          (take-higher (keys candidates))
          s/Str)))))

(defn ^:no-doc is-header?
  [line schema]
  (if (> (count (filter nil? line)) 0)
    false
    (let [full-schema (mapv #(s/one % "toto") schema)
          coerce-line (c/coercer full-schema csv-coercer)]
      (try
        (let [coerced (coerce-line line)]
          (s/validate full-schema coerced)
          false)
        (catch Exception e
          true)))))

(defn ^:no-doc is-quoted?
  [lines delimiter]
  (let [quoted-delim (java.util.regex.Pattern/quote (str delimiter))
        regexp (re-pattern (str (format "\"%s|%s\"" quoted-delim quoted-delim)))]
    ;; skip the first line in case it's a header
    (every? #(re-find regexp %) (take 10 (rest lines)))))

(defn ^:no-doc analyze-csv
  [uri lookahead]
  (when (instance? Reader uri)
    (let [^Reader rdr uri]
      (if (.markSupported rdr)
       (.mark rdr 1000000)
       (throw (Exception. "Cannot analyze csv from unmarkable reader")))))
  (let [^BufferedReader rdr (io/reader uri)]
    (try
      (let [lines (loop [ls []]
                    (if-let [line (.readLine rdr)]
                      (if (< (count ls) lookahead)
                        (recur (conj ls line))
                        ls)
                      ls))
            possible-header (first lines)
            delimiter (guess-delimiter lines)
            seg-lines (parse-fields (rest lines) delimiter)
            fields-schema (guess-types seg-lines)
            has-header? (is-header? (first (parse-fields [possible-header] delimiter)) fields-schema)
            quoted? (is-quoted? lines delimiter)]
        (if (instance? Reader uri)
          (let [^Reader typed-rdr uri]
            (.reset typed-rdr)))
        {:delimiter delimiter :fields-schema fields-schema :header has-header? :quoted? quoted?})
      (finally
        (if-not (instance? Reader uri)
          (.close rdr))))))

(defn ^:no-doc wrap-with-counter
  [f step]
  (let [total (atom 0)]
    (fn []
      (swap! total inc)
      (when (= 0 (rem @total step))
        (log/info "Processed" @total "lines"))
      (f))))

(defn ^:no-doc make-read-fn
  [f {:keys [strict? silent?]
      :or {strict? true}}]
  (if strict?
    f
    (let [m (meta f)]
      (->
       (fn [rdr]
         (let [res (try
                     (f rdr)
                     (catch Exception e
                       (if-not silent?
                         (log/error e "Error while reading csv"))))]
           (if res
             res
             ;; read next line if there was an exception
             (recur rdr))))
       (vary-meta (fn [o n] (merge n o)) m)))))

(defn close!
  "Closes and cleans the io ressources used. Can be used on a greedy generator or any
line of a lazy seqs of results"
  [f]
  (when-let [clean (get (meta f) ::clean-reader)]
    (clean)))

(defn guess-spec
  "This function takes a source of csv lines (either a *Reader*, *InputStream* or *String* URI)
and tries to guess the specs necessary to parse it. You can use the option map to specify some values
for the spec. Recognised options are:

 *Analysis options*

 +  **header?**: Whether the file as a header on the first line
 +  **sample-size**: number of lines on which heuristics are applied. Defaults to *100*
 +  **guess-types?**: Whether to try to guess types for each field

 *File options*

 +  **encoding**: Character encoding for the file

 *Processing options*

 +  **field-names-fn**: fn to apply to the name of each field. Defaults to trim function
 +  **nullable-fields?**: Whether the values are optionals. Defaults to *true*
 +  **keywordize-keys?**: Whether to turn fields into keywords. Defaults to *true*

 *Format options*

 +  **delimiter**: Character used as a delimiter
 +  **schema**: Schema to validate and coerce output
 +  **field-names**: Names for the csv fields, in order"
  ([uri
    {:keys [header? field-names field-names-fn schema encoding
            guess-types? delimiter nullable-fields? keywordize-keys?
            sample-size bom]
     :or {guess-types? true
          field-names-fn str/trim
          nullable-fields? true
          keywordize-keys? true
          sample-size 100}
     :as opts}]
     (when-not (or (instance? String uri)
                   (instance? File uri))
       (throw (ex-info "Cannot guess the specs of inputs that are neither String path nor File")))
     (let [[^Reader rdr clean-rdr enc bom-name] (get-reader uri encoding bom)]
       (try
         (let [{guessed-schema :fields-schema
                guessed-delimiter :delimiter
                guessed-header :header
                guessed-quote :quoted?
                :as analysis} (try
                                (analyze-csv rdr sample-size)
                                (catch Exception e
                                  (throw e)
                                  {}))
                ^CsvPreference pref-opts (csv-prefs (merge analysis opts))
                vec-output (not (or (get opts :header? guessed-header) field-names))
                ^AbstractCsvReader csv-rdr (if vec-output
                                             (CsvListReader. rdr pref-opts)
                                             (CsvMapReader. rdr pref-opts))
                fnames (when-not vec-output
                         (mapv field-names-fn
                               (cond
                                (or header? guessed-header) (.getHeader csv-rdr true)
                                field-names field-names)))
                wrap-types (if nullable-fields? #(maybe %) identity)
                wrap-keys (if keywordize-keys? (comp keyword str/trim) str/trim)
                full-specs (if vec-output
                             (let [infered-schema (map-indexed (fn [idx t] (s/one (wrap-types t) (str "col" idx))) guessed-schema)]
                               (if (and guess-types? (vector? schema) (= (count schema) (count infered-schema)))
                                 (into [] (map (fn [given guessed] (if (nil? given) guessed given)) schema infered-schema))
                                 (into [] infered-schema)))
                             (let [infered-schema (zipmap (map wrap-keys fnames) (map wrap-types guessed-schema))]
                               (if guess-types?
                                 (merge infered-schema schema)
                                 infered-schema)))]
           (merge {:schema full-specs :field-names fnames :delimiter (or delimiter guessed-delimiter)
                   :encoding enc :skip-analysis? true :header? guessed-header :quoted? guessed-quote :bom bom-name} opts))
         (finally
           (clean-rdr rdr)))))
  ([uri] (guess-spec uri {})))

(defn ^:no-doc guess-possible?
  [rdr]
  (try
    (if-not (instance? java.io.Reader rdr)
      true
      (let [^Reader typed-rdr rdr]
        (.markSupported typed-rdr)))
    (catch Exception _ false)))

(defn csv-line-reader
  "This function returns a reader function that accepts inputs of one *String* line at a time.
It takes the same options as [[read-csv]] minus some processing and the file and analysis options."
  [{:keys [header? field-names field-names-fn schema
           guess-types? strict? counter-step
           silent? limit nullable-fields?
           keywordize-keys? coercers]
     :or {guess-types? true
          strict? true
          field-names-fn str/trim
          keywordize-keys? true
          coercers {}}
     :as opts}]
  (let [fnames-arr (into-array String (map name field-names))
        pref-opts (csv-prefs opts)
        read-fn line-read-fn
        vec-output (not (or header? field-names))
        parse-csv (if (empty? schema)
                    identity
                    (c/coercer schema (merge-coercers :input csv-coercer coercers)))
        read-map (if (and (not vec-output) keywordize-keys?)
                   (comp parse-csv keywordize-keys)
                   parse-csv)
        res-fn (if vec-output
                 (read-fn (fn [^Reader rdr]
                            (with-open [csv-rdr (CsvListReader. rdr pref-opts)]
                              (.read csv-rdr)))
                          (fn [e] (parse-csv (into [] e))))
                 (read-fn (fn [^Reader rdr]
                            (with-open [csv-rdr (CsvMapReader. rdr pref-opts)]
                              (.read csv-rdr fnames-arr)))
                          (fn [e]
                            (read-map (into {} e)))))]
    res-fn))

(defn read-csv
  "This function takes a source of csv lines (either a *Reader*, *InputStream* or *String* URI)
 and returns the parsed results. If headers are found on the file or *field-names* where given
 as options, it will return a collection of one map per line, associating each field name with
 its value. If not,   one vector will be returned for each line, in order.

 You can use the option map to specify some values for the spec. Recognised options are:

 *Analysis options*

 +  **header?**: Whether the file as a header on the first line
 +  **sample-size**: number of lines on which heuristics are applied. Defaults to *100*
 +  **guess-types?**: Whether to try to guess types for each field. Defaults tu *true*
 +  **skip-analysis?**: Whether to completely bypass analysis and only use spec
    Defaults to *false*

 *Processing options*

 +  **greedy?**: If true returns a function that can be used as a generator, returning one line
    with each call, else returns a lazy seq of lines. Defaults to *false*
 +  **strict?**: Whether to throw exception on reading of validation error, or just skip it.
    Defaults to *true*
 +  **silent?**: Whether there should be error messages emitted on *stderr* when skipping Exceptions.
    Defaults to *false*
 +  **limit**: Closes and cleans the io ressources after reading this many lines. Useful for
    sampling
 +  **field-names-fn**: fn to apply to the name of each field. Can be used to sanitize header
    names. Defaults to trim function
 +  **nullable-fields?**: Whether the values are optionals. Defaults to *true*
 +  **keywordize-keys?**: Whether to turn fields into keywords. Defaults to *true*

 *Format options*

 +  **delimiter**: Character used as a delimiter
 +  **schema**: Schema to validate and coerce output
 +  **coercers**: A map associating a type as key with a function from *String* to that type.
    For example:

        {:coercers {java.util.Date (make-date-coercer \"yyyyMMdd\")}}

 +  **field-names**: Names for the csv fields, in order"
  ([uri
    {:keys [header? field-names field-names-fn schema encoding
            guess-types? strict? greedy? counter-step
            silent? limit skip-analysis? nullable-fields?
            keywordize-keys? coercers bom]
     :or {guess-types? true
          strict? true
          field-names-fn str/trim
          nullable-fields? true
          keywordize-keys? true
          coercers {}}
     :as opts}]
     (let [[^Reader rdr clean-rdr enc] (get-reader uri encoding bom)]
       (try
         (let [{:keys [header? field-names field-names-fn schema encoding
                       guess-types? strict? greedy? counter-step
                       silent? limit skip-analysis?]
                :or {guess-types? true
                     strict? true
                     field-names-fn str/trim}
                :as full-spec} (if skip-analysis?
                                 opts
                                 (guess-spec uri opts))
                fnames-arr (into-array String (map name field-names))
                read-fn (if greedy? greedy-read-fn read-row)
                vec-output (not (or header? field-names))
                ^CsvPreference pref-opts (csv-prefs (merge full-spec opts))
                ^AbstractCsvReader csv-rdr (if vec-output
                                             (CsvListReader. rdr pref-opts)
                                             (CsvMapReader. rdr pref-opts))
                _ (when header? (.getHeader csv-rdr true))]
           (let [parse-csv (if (empty? schema)
                             identity
                             (c/coercer schema (merge-coercers :input csv-coercer coercers)))
                 read-map (if (and (not vec-output) keywordize-keys?)
                            (comp parse-csv keywordize-keys)
                            parse-csv)
                 res-fn (if vec-output
                          (read-fn csv-rdr
                                   (make-read-fn (fn [^CsvListReader rdr] (.read rdr))
                                                 {:strict? strict?
                                                  :silent? silent?})
                                   (fn [e] (parse-csv (into [] e)))
                                   clean-rdr
                                   limit)
                          (read-fn csv-rdr
                                   (make-read-fn (fn [^CsvMapReader rdr] (.read rdr fnames-arr))
                                                 {:strict? strict?
                                                  :silent? silent?})
                                   (fn [e] (read-map e))
                                   clean-rdr
                                   limit))]
             (cond
              counter-step (wrap-with-counter res-fn counter-step)
              :default res-fn)))
         (catch Exception e
           (clean-rdr rdr)
           (throw e)))))
  ([uri] (read-csv uri {})))

(defn ^:no-doc get-writer
  [tgt bom options]
  (cond
   (instance? Writer tgt)
   [tgt (fn [& _] nil)]

   (string? tgt)
   (let [stream (apply io/output-stream tgt (apply concat options))
         bom-bts (get bom-bytes bom)]
     (doseq [b bom-bts]
       (.write stream b))
     (let [wrt (apply io/writer stream (apply concat options))]
       [wrt (fn [& all]
              (doseq [r all] (.close r))
              (.close wrt))]))

   :default
   [(apply io/writer tgt (apply concat options)) (fn [& _] nil)]))

(defn ^:no-doc fields-list
  ([field-names sample-names position]
     (let [all-spec-fields (into #{} field-names)
           all-sample-fields (into #{} (map name sample-names))
           [removed added _] (data/diff all-spec-fields all-sample-fields)
           base-list (remove (into #{} removed) field-names)]
       (case position
         :before (into [] (concat (sort added) base-list))
         :after (into [] (concat base-list (sort added))))))
  ([field-names sample-names] (fields-list field-names sample-names :after)))

(defn write-csv!
  "This function takes a target (either a *Reader*, *InputStream* or *String* URI), a spec of options
 and a seq of either *maps* or *seqs*. Each data structure is written to the target csv
 as a single line.

 Right now, custom encoders are ignored and each value is just coerced to its *String* format.

 The option map is the same as [[read-csv]] with these added options:

 *Write options*

 +  **append**: if set to true, opens the given uri in append mode. *header?* option is then
    ignored."
  ([uri
    {:keys [bom encoding field-names coercers header? append] :as spec
     :or {bom :none
          header? true}}
    data]
     (let [[wrt clean-writer] (get-writer uri bom (select-keys spec [:encoding :append]))
           prefs (csv-prefs spec)
           sample (first data)
           seq-input? (sequential? sample)
           [csv-writer write-fn] (if seq-input?
                                   (let [csv-wrt (CsvListWriter. wrt prefs)]
                                     [csv-wrt (fn [^CsvListWriter wrt l] (.write wrt (map str l)))])
                                   (let [csv-wrt (CsvMapWriter. wrt prefs)
                                         fields (fields-list field-names (keys sample))
                                         fields-array (into-array String fields)]
                                     (when (and header?
                                                (not append))
                                       (.writeHeader csv-wrt fields-array))
                                     [csv-wrt (fn [^CsvMapWriter wrt m]
                                                (let [to-write (reduce (fn [acc [k v]]
                                                                         (assoc! acc (name k) (str v)))
                                                                       (transient {}) m)]
                                                  (.write wrt (persistent! to-write) fields-array)))]))
           ;; out-coercers (merge-coercers :output csv-coercer coercers)
           ]
       (try
         (doseq [l data
                :when l]
           (write-fn csv-writer l))
         (catch Exception e
           (log/error e "Error writing csv file"))
         (finally
           (.flush csv-writer)
           (clean-writer csv-writer)))))
  ([uri data] (write-csv! uri {} data)))

(defn csv-line
  [spec data]
  (let [wrt (StringWriter.)
        full-spec (assoc spec :header? false)]
    (write-csv! wrt full-spec [data])
    (.flush wrt)
    (let [line (.toString wrt)]
      line)))
