(ns ultra-csv.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [schema [core :as s] [coerce :as c]])
  (:import [org.supercsv.io CsvMapReader CsvListReader]
           [org.supercsv.prefs CsvPreference CsvPreference$Builder]
           [java.io Reader InputStream]
           [java.nio.charset Charset]
           [com.ibm.icu.text CharsetDetector]
           [org.apache.commons.io.input BOMInputStream]
           [org.apache.commons.io ByteOrderMark]))

(declare analyze-csv)

(def available-charsets (into #{} (.keySet (Charset/availableCharsets))))

(defn guess-charset
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

(defn get-reader
  ([src encoding]
     (if (instance? java.io.Reader src)
       [src (fn [] nil) encoding]
       (let [boms (into-array ByteOrderMark 
                              [ByteOrderMark/UTF_16LE ByteOrderMark/UTF_16BE
                               ByteOrderMark/UTF_32LE ByteOrderMark/UTF_32BE
                               ByteOrderMark/UTF_8])]
         (let [istream (io/input-stream src)
               enc (or encoding (guess-charset istream))
               rdr (io/reader (BOMInputStream. istream boms) :encoding enc)]
           [rdr
            (fn [] (do (.close rdr)
                       (.close istream)
                       true))
            enc]))))
  ([src] (get-reader src nil)))

(defn- read-row
  [rdr read-from-csv transform-line clean-rdr limit]
  (let [res (read-from-csv rdr)]
    (if (and res (or (nil? limit) (< (.getLineNumber rdr) limit)))
      (cons
       (vary-meta (transform-line res) assoc ::csv-reader rdr ::clean-reader clean-rdr)
       (lazy-seq (read-row rdr read-from-csv transform-line clean-rdr limit)))
      (clean-rdr)
      )))

(defn- greedy-read-fn
  [rdr read-from-csv transform-line clean-rdr limit]
  (->
   (fn []
     (let [res (read-from-csv rdr)]
       (if (and res (or (nil? limit) (< (.getLineNumber rdr) limit)))
         (transform-line res)
         (clean-rdr))))
   (vary-meta assoc ::csv-reader rdr ::clean-reader clean-rdr)))

(defn- line-read-fn
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

(defn find-char-pos
  [line char]
  (loop [found []
         cur 0]
    (let [pos (.indexOf line (int char) cur)]
      (if (not= pos -1)
        (recur (conj found pos) (inc pos))
        found))))

(defn guess-delimiter
  [lines]
  (let [all-dels (for [line lines
                       :let [clean-line (str/replace line #"\"[^\"]*\"" "")]]
                   (into {}
                         (map
                          (fn [character]
                            [character
                             (count (find-char-pos clean-line character))])
                          [\, \; \space \tab])))
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

(defprotocol Iprefs
  (make-prefs [arg] "generates a CsvPreference object"))

(defn csv-prefs
  [{:keys [quote-symbol delimiter end-of-lines]
    :or {quote-symbol \"
         end-of-lines "\n"
         delimiter \,}}]
  (->
   (CsvPreference$Builder. quote-symbol (int delimiter) end-of-lines)
   (.build)))

(extend-protocol Iprefs
  CsvPreference
  (make-prefs [this] this)
  clojure.lang.IPersistentMap
  (make-prefs [this] (csv-prefs this))
  clojure.lang.Keyword
  (make-prefs [this]
    (case this
      :standard CsvPreference/STANDARD_PREFERENCE
      :excel CsvPreference/EXCEL_PREFERENCE
      :excel-north-europe CsvPreference/EXCEL_NORTH_EUROPE_PREFERENCE
      :tab CsvPreference/TAB_PREFERENCE
      (throw (IllegalArgumentException. (format "keyword [ %s ] is not recognised" this))))))

(defn parse-fields
  [lines delimiter]
  (let [txt (str/join "\n" lines)
        prefs (make-prefs {:delimiter delimiter})]
    (with-open [rdr (java.io.StringReader. txt)]
      (let [listr (CsvListReader. rdr prefs)
            _ (.getHeader listr false)
            seg-lines (loop [out []]
                        (if-let [fields (.read listr)]
                          (recur (conj out (into [] fields)))
                          out))]
        (.close listr)
        seg-lines))))

(defn int-string?
  [s]
  (if (re-matches #"-?\d+" s)
    true false))

(defn double-string?
  [s]
  (if (re-matches #"-?\d+([\.,]\d+)?" s)
    true false))

(def Num java.lang.Double)

(def known-types
  [[s/Int int-string?]
   [Num double-string?]])

(def csv-coercer
  {s/Int (fn [s] (Long/parseLong s))
   Num (fn [s] (Double/parseDouble s))
   s/Keyword (fn [s] (keyword s))})

(defn take-higher
  [cands]
  (if (empty? cands)
    s/Str
    (let [lookfor (into #{} cands)]
      (first (remove nil? (filter #(contains? lookfor %) (map first known-types)))))))

(defn keywordize-keys
  [hm]
  (persistent!
   (reduce (fn [acc [k v]]
             (assoc! acc (keyword k) v))
           (transient {}) hm)))

(defn guess-types
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

(defn analyze-csv
  [uri lookahead]
  (when (instance? Reader uri)
    (if (.markSupported uri)
      (.mark uri 1000000)
      (throw (Exception. "Cannot analyze csv from unmarkable reader"))))
  (let [rdr (io/reader uri)]
    (try
      (let [lines (loop [ls []]
                    (if-let [line (.readLine rdr)]
                      (if (< (count ls) lookahead)
                        (recur (conj ls line))
                        ls)
                      ls))
            delimiter (guess-delimiter lines)
            seg-lines (parse-fields lines delimiter)
            fields-schema (guess-types seg-lines)]
        (if (instance? Reader uri)
          (.reset uri))
        {:delimiter delimiter :fields-schema fields-schema})
      (finally
        (if-not (instance? Reader uri)
          (.close rdr))))))

(defn wrap-with-counter
  [f step]
  (let [total (atom 0)]
    (fn []
      (swap! total inc)
      (when (= 0 (rem @total step))
        (println "Processed" @total "lines"))
      (f))))

(defn make-read-fn
  [f {:keys [strict? silent?]
      :or {strict? true}}]
  (if strict?
    f
    (let [m (meta f)]
      (->
       (fn []
         (let [res (try
                     (f)
                     (catch Exception e
                       (if-not silent?
                         (println e "EXCEPTION" e))
                       e))]
           (if (instance? Exception res)
             (recur)
             res)))
       (vary-meta (fn [o n] (merge n o)) m)))))

(defn close!
  [f]
  (when-let [clean (get (meta f) ::clean-reader)]
    (clean)))

(defn guess-spec
  ([uri
    {:keys [preference header field-names field-names-fn schema encoding
            guess-types? delimiter nullable-fields? keywordize-keys?]
     :or {header true
          guess-types? true
          field-names-fn str/trim
          nullable-fields? true
          keywordize-keys? true}
     :as opts}]
     (let [[rdr clean-rdr enc] (get-reader uri encoding)]
       (try
         (let [resettable? (.markSupported rdr)
               {guessed-schema :fields-schema
                guessed-delimiter :delimiter
                :as analysis} (try
                                (analyze-csv rdr 100)
                                (catch Exception e
                                  (throw e)
                                  {}))
                ^CsvPreference pref-opts (make-prefs (merge
                                                      analysis
                                                      (get opts :preference
                                                           (assoc opts :uri uri))))
               vec-output (not (or header field-names))
               csv-rdr (if vec-output
                         (CsvListReader. rdr pref-opts)
                         (CsvMapReader. rdr pref-opts))
               fnames (when-not vec-output
                        (map field-names-fn
                             (cond
                              header (.getHeader csv-rdr true)
                              field-names field-names)))
               wrap-types (if nullable-fields? #(s/maybe %) identity)
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
                   :encoding enc :skip-analysis? true :header (true? header)} opts))
         (catch Exception e
           (println (format "error while reading: %s" (str e)))
           (throw e)
           )
         (finally
           (clean-rdr)))))
  ([uri] (guess-spec uri {})))

(defn guess-possible?
  [rdr]
  (try
    (if-not (instance? java.io.Reader rdr)
     true
     (.markSupported rdr))
    (catch Exception _ false)))

(defn csv-line-reader
  [{:keys [preference header field-names field-names-fn schema encoding
           guess-types? strict? greedy? counter-step
           silent? limit skip-analysis? nullable-fields?
           keywordize-keys?]
     :or {header true guess-types? true
          strict? true
          field-names-fn str/trim}
     :as opts}]
  (let [fnames-arr (into-array String (map name field-names))
        ^CsvPreference pref-opts (make-prefs opts)
        read-fn line-read-fn
        vec-output (not (or header field-names))
        parse-csv (c/coercer schema csv-coercer)
        read-map (if (and (not vec-output) keywordize-keys?) 
                   (comp parse-csv keywordize-keys)
                   parse-csv)
        res-fn (if vec-output
                 (read-fn (fn [rdr]
                            (with-open [csv-rdr (CsvListReader. rdr pref-opts)]
                              (.read csv-rdr)))
                          (fn [e] (parse-csv (into [] e))))
                 (read-fn (fn [rdr]
                            (with-open [csv-rdr (CsvMapReader. rdr pref-opts)]
                              (.read csv-rdr fnames-arr)))
                          (fn [e] 
                            (read-map e))))]
    res-fn))

(defn read-csv
  ([uri
    {:keys [preference header field-names field-names-fn schema encoding
            guess-types? strict? greedy? counter-step
            silent? limit skip-analysis? nullable-fields?
            keywordize-keys?]
     :or {header true guess-types? true
          strict? true
          field-names-fn str/trim
          nullable-fields? true
          keywordize-keys? true}
     :as opts}]
     (let [guess-allowed? (guess-possible? uri)]
       (if (and (or (not skip-analysis?)
                    guess-types)
                (not guess-allowed?))
         (throw (ex-info (format "Input of class %s cannot be reset, cannot guess its specs" (class uri))))
         (let [[rdr clean-rdr enc] (get-reader uri encoding)]
           (try
             (let [resettable? (.markSupported rdr)
                   {:keys [preference header field-names field-names-fn schema encoding
                           guess-types? strict? greedy? counter-step
                           silent? limit skip-analysis?]
                    :or {header true guess-types? true
                         strict? true
                         field-names-fn str/trim}
                    :as full-spec} (if skip-analysis?
                                     opts
                                     (guess-spec uri opts))
                   fnames-arr (into-array String (map name field-names))
                   read-fn (if greedy? greedy-read-fn read-row)
                   vec-output (not (or header field-names))
                   ^CsvPreference pref-opts (make-prefs (merge
                                                         full-spec
                                                         (get opts :preference
                                                              (assoc opts :uri uri))))
                   csv-rdr (if vec-output
                             (CsvListReader. rdr pref-opts)
                             (CsvMapReader. rdr pref-opts))
                   _ (if header (.getHeader csv-rdr true))]
               (let [parse-csv (c/coercer schema csv-coercer)
                     read-map (if (and (not vec-output) keywordize-keys?)
                                (comp parse-csv keywordize-keys)
                                parse-csv)
                     res-fn (if vec-output
                              (read-fn csv-rdr
                                       (make-read-fn #(.read %)
                                                     {:strict? strict?
                                                      :silent? silent?})
                                       (fn [e] (parse-csv (into [] e)))
                                       clean-rdr
                                       limit)
                              (read-fn csv-rdr
                                       (make-read-fn #(.read % fnames-arr)
                                                     {:strict? strict?
                                                      :silent? silent?})
                                       (fn [e] (read-map e))
                                       clean-rdr
                                       limit))]
                 (cond
                  counter-step (wrap-with-counter res-fn counter-step)
                  :default res-fn)))
             (catch Exception e
               (println (format "error while reading: %s" (str e)))
               (clean-rdr)
               (throw e)))))))
  ([uri] (read-csv uri {})))
