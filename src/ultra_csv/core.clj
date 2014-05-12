(ns ultra-csv.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import [org.supercsv.io CsvMapReader CsvListReader]
           [org.supercsv.prefs CsvPreference CsvPreference$Builder]
           [org.supercsv.cellprocessor.ift CellProcessor]
           [org.supercsv.cellprocessor Optional]
           [java.io Reader InputStream]
           [java.nio.charset Charset]
           [com.ibm.icu.text CharsetDetector]
           [org.apache.commons.io.input BOMInputStream]
           [org.apache.commons.io ByteOrderMark]))

(declare analyze-csv)

(defn- read-row
  [rdr read-from-csv transform-line limit]
  (let [res (read-from-csv)]
    (if (and res (or (nil? limit) (< (.getLineNumber rdr) limit)))
      (cons
       (vary-meta (transform-line res) assoc ::csv-reader rdr)
       (lazy-seq (read-row rdr read-from-csv transform-line limit)))
      (.close rdr))))

(defn- greedy-read-fn
  [rdr read-from-csv transform-line limit]
  (->
   (fn []
     (let [res (read-from-csv)]
       (if (and res (or (nil? limit) (< (.getLineNumber rdr) limit)))
         (transform-line res)
         (.close rdr))))
   (vary-meta assoc ::csv-reader rdr)))

(def processor-types
  {:long org.supercsv.cellprocessor.ParseLong
   :not-null org.supercsv.cellprocessor.constraint.NotNull
   :double org.supercsv.cellprocessor.ParseDouble
   :optional org.supercsv.cellprocessor.Optional})

(defn processor-specs
  [specs]
  (into {}
        (for [[k processor-names] specs]
          (let [processor
                (loop [todo (reverse processor-names)
                       pr nil]
                  (if-let [nam (first todo)]
                    (let [klass (get processor-types nam)]
                      (if (nil? pr)
                        (let [cst (.getConstructor klass (make-array Class 0))
                              proc (.newInstance cst (make-array Object 0))]
                          (recur (rest todo) proc))
                        (let [csts (.getConstructors klass)
                              cst (first
                                   (filter
                                    (fn [c] (= 1 (count (.getParameterTypes c)))) csts))
                              proc (.newInstance cst (into-array Object [pr]))]
                          (recur (rest todo) proc))))
                    pr))]
            [(str/trim (name k)) processor]))))

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
                       ;; :let [clean-line (str/replace line #"\"[^\"]*\"" "")]
                       ]
                   (into {}
                         (map
                          (fn [character]
                            [character
                             (count (find-char-pos line character))])
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
  [{:keys [quote-symbol delimiter end-of-lines uri specs]
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

(defn long-string?
  [s]
  (if (re-matches #"\d+" s)
    true false))

(defn double-string?
  [s]
  (if (re-matches #"-?\d+([\.,]\d+)?" s)
    true false))

(def known-types
  [[:long long-string?]
   [:double double-string?]])

(defn take-higher
  [cands]
  (if (empty? cands)
    []
    (let [lookfor (into #{} cands)]
      [:optional (first (remove nil? (filter #(contains? lookfor %) (map first known-types))))])))

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
          [])))))

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
            procs (guess-types seg-lines)]
        (if (instance? Reader uri)
          (.reset uri))
        {:delimiter delimiter :processors procs})
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
  (when-let [rdr (get (meta f) ::csv-reader)]
    (.close rdr)))

(defn get-reader
  ([src encoding]
     (if (instance? java.io.Reader src)
       [src (fn [] nil) encoding]
       (let [boms (into-array ByteOrderMark 
                              [ByteOrderMark/UTF_16LE ByteOrderMark/UTF_16BE
                               ByteOrderMark/UTF_32LE ByteOrderMark/UTF_32BE
                               ByteOrderMark/UTF_8])]
         (with-open [istream (io/input-stream src)]
           (let [enc (or encoding (guess-charset istream))
                 content (slurp (BOMInputStream. istream boms) :encoding enc)
                 rdr (java.io.StringReader. content)]
             [rdr (fn [] (do (.close rdr) true)) enc])))))
  ([src] (get-reader src nil)))

(defn guess-spec
  ([uri
    {:keys [preference header field-names field-names-fn specs encoding
            guess-types? delimiter]
     :or {header true
          guess-types? true
          field-names-fn str/trim}
     :as opts}]
     (let [[rdr clean-rdr enc] (get-reader uri encoding)]
       (try
         (let [resettable? (.markSupported rdr)
               {guessed-procs :processors
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
               fnames (map field-names-fn
                           (cond
                            header (.getHeader csv-rdr true)
                            field-names field-names
                            (> (count guessed-procs) 0) (range (count guessed-procs))))
               full-specs (if guess-types?
                            (merge (zipmap fnames guessed-procs) specs)
                            specs)]
           (merge {:specs full-specs :field-names fnames :delimiter (or delimiter guessed-delimiter)
                   :encoding enc :skip-analysis? true :header (true? header)} opts))
         (catch Exception e
           (println (format "error while reading: %s" (str e)))
           ;; (throw e)
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

(defn read-csv
  ([uri
    {:keys [preference header field-names field-names-fn specs encoding
            guess-types? strict? greedy? counter-step
            silent? limit skip-analysis?]
     :or {header true guess-types? true
          strict? true
          field-names-fn str/trim}
     :as opts}]
     (let [guess-allowed? (guess-possible? uri)]
       (if (and (or (not skip-analysis?)
                    guess-types)
                (not guess-allowed?))
         (throw (ex-info (format "Input of class %s cannot be reset, cannot guess its specs" (class uri))))
         (let [[rdr clean-rdr enc] (get-reader uri encoding)]
           (try
             (let [resettable? (.markSupported rdr)
                   {:keys [preference header field-names field-names-fn specs encoding
                           guess-types? strict? greedy? counter-step
                           silent? limit skip-analysis?]
                    :or {header true guess-types? true
                         strict? true
                         field-names-fn str/trim}
                    :as full-spec} (if skip-analysis?
                                     opts
                                     (guess-spec uri opts))
                   fnames-arr (into-array String (map name field-names))
                   processors (make-array CellProcessor (count field-names))
                   specs-proc (processor-specs specs)
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
               (doseq [[i nam] (map-indexed (fn [i v] [i v]) field-names)]
                 (let [proc (get specs-proc nam (Optional.))]
                   (aset processors i proc)))
               (let [res-fn (if vec-output
                              (read-fn csv-rdr
                                       (make-read-fn #(.read csv-rdr processors)
                                                     {:strict? strict?
                                                      :silent? silent?})
                                       (fn [e] (into [] e))
                                       limit)
                              (read-fn csv-rdr
                                       (make-read-fn #(.read csv-rdr fnames-arr processors)
                                                     {:strict? strict?
                                                      :silent? silent?})
                                       (fn [e] (->> e
                                                    (into {})
                                                    (walk/keywordize-keys)))
                                       limit))]
                 (cond
                  counter-step (wrap-with-counter res-fn counter-step)
                  :default res-fn)))
             (catch Exception e
               (println (format "error while reading: %s" (str e)))
               ;; (throw e)
               )
             (finally
               (clean-rdr)))))))
  ([uri] (read-csv uri {})))
