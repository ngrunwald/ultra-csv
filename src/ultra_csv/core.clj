(ns ultra-csv.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import [org.supercsv.io CsvMapReader CsvListReader]
           [org.supercsv.prefs CsvPreference CsvPreference$Builder]
           [org.supercsv.cellprocessor.ift CellProcessor]
           [org.supercsv.cellprocessor Optional]
           [java.io Reader]))

(declare analyze-csv)

(defn- read-row
  [rdr read-from-csv transform-line]
  (let [res (read-from-csv)]
    (if res
      (cons (transform-line res) (lazy-seq (read-row rdr read-from-csv transform-line)))
      (do (.close rdr)
          nil))))

(defn- greedy-read-fn
  [rdr read-from-csv transform-line]
  (->
   (fn []
     (let [res (read-from-csv)]
       (if res
         (transform-line res)
         (do (.close rdr)
             nil))))
   (vary-meta assoc ::csv-reader rdr)))

(def processor-types
  {:long org.supercsv.cellprocessor.ParseLong
   :not-null org.supercsv.cellprocessor.constraint.NotNull
   })

(defn processor-specs
  [specs]
  (into {}
        (for [[k processor-names] specs]
          (let [processor
                (loop [todo processor-names
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
                       :let [clean-line (str/replace line #"\"[^\"]+\"" "")]]
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
  (make-prefs [arg analysis] "generates a CsvPreference object"))

(defn csv-prefs
  [{:keys [quote-symbol delimiter end-of-lines uri specs]
    :or {quote-symbol \" end-of-lines "\n"}}
   {guessed-delimiter :delimiter
    procs :processors}]
  (let [del (or delimiter guessed-delimiter \,)]
    (->
     (CsvPreference$Builder. quote-symbol (int del) end-of-lines)
     (.build))))

(extend-protocol Iprefs
  CsvPreference
  (make-prefs [this _] this)
  clojure.lang.IPersistentMap
  (make-prefs [this analysis] (csv-prefs this analysis))
  clojure.lang.Keyword
  (make-prefs [this _]
    (case this
      :standard CsvPreference/STANDARD_PREFERENCE
      :excel CsvPreference/EXCEL_PREFERENCE
      :excel-north-europe CsvPreference/EXCEL_NORTH_EUROPE_PREFERENCE
      :tab CsvPreference/TAB_PREFERENCE
      (throw (IllegalArgumentException. (format "keyword [ %s ] is not recognised" this))))))

(defn parse-fields
  [lines delimiter]
  (let [txt (str/join "\n" lines)
        prefs (make-prefs {:delimiter delimiter} {})]
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

(def known-types
  [[:long long-string?]])

(defn guess-types
  [lines]
  (for [offset (range (count (first lines)))]
    (loop [todo lines
           candidates known-types]
      (if-let [line (first todo)]
        (let [field (nth line offset)
              valid (reduce (fn [acc [typ f]]
                              (if (and field (f field))
                               (assoc acc typ f)
                               acc))
                            {} candidates)]
          (recur (rest todo) valid))
        (keys candidates)))))

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

(defn read-csv
  ([uri
    {:keys [preference header field-names specs encoding
            guess-types? strict? greedy? counter-step
            silent?]
     :or {encoding "utf-8" header true guess-types? true
          strict? true}
     :as opts}]
     (let [rdr (io/reader uri :encoding encoding)]
       (try
         (let [{guessed-procs :processors :as analysis} (try
                                                          (analyze-csv uri 100)
                                                          (catch Exception e
                                                            {}))
               ^CsvPreference pref-opts (make-prefs (get opts :preference
                                                         (assoc opts :uri uri))
                                                    analysis)
               vec-output (not (or header field-names))
               csv-rdr (if vec-output
                         (CsvListReader. rdr pref-opts)
                         (CsvMapReader. rdr pref-opts))
               fnames (map str/trim
                           (cond
                            header (.getHeader csv-rdr true)
                            field-names field-names
                            (> (count guessed-procs) 0) (range (count guessed-procs))))
               guessed-fields-procs (zipmap fnames guessed-procs)
               fnames-arr (into-array String (map name fnames))
               processors (make-array CellProcessor (count fnames))
               specs-proc (processor-specs (if guess-types?
                                             (merge guessed-fields-procs specs)
                                             specs))
               read-fn (if greedy? greedy-read-fn read-row)]
           (doseq [[i nam] (map-indexed (fn [i v] [i v]) fnames)]
             (let [proc (get specs-proc nam (Optional.))]
               (aset processors i proc)))
           (let [res-fn (if vec-output
                          (read-fn csv-rdr
                                   (make-read-fn #(.read csv-rdr processors)
                                                 {:strict? strict?
                                                  :silent? silent?})
                                   (fn [e] (into [] e)))
                          (read-fn csv-rdr
                                   (make-read-fn #(.read csv-rdr fnames-arr processors)
                                                 {:strict? strict?
                                                  :silent? silent?})
                                   (fn [e] (->> e
                                               (into {})
                                               (walk/keywordize-keys)))))]
             (cond
              counter-step (wrap-with-counter res-fn counter-step)
              :default res-fn)))
         (catch Exception e
           (.close rdr)
           (throw e)))))
  ([uri] (read-csv uri {})))

