(ns core
  (:require [clj-http.client :as client]
            [clojure.data.json :as json]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as s]
            [clojure.tools.cli :refer [parse-opts]]
            [next.jdbc :as jdbc])
  (:import net.snowflake.client.jdbc.SnowflakeDriver
           java.sql.DriverManager
           java.time.LocalDateTime
           java.time.LocalDate
           java.time.LocalTime
           java.time.format.DateTimeFormatter)
  (:gen-class))

;; =========
;; Watersignal

(defn try-get
  [url]
  (try (client/get url)
       (catch Exception e
         {:status :exception
          :body (.getMessage e)})))

(defn get-meter-data
  [authkey meterid sdate edate compound retry-n wait-secs]
  (let [url (str "https://www.watersignal.com/admin2/api/meterdata/authid/" authkey
                 "/meterid/" meterid "/sdate/" sdate "/edate/" edate "/compound/" compound)
        {:keys [status body]} (try-get url)]
    (case status
      200 body
      (if (pos? retry-n)
        (do
          (println "API call failed. " retry-n  " retries left. Retrying in " wait-secs " seconds...")
          (Thread/sleep (* wait-secs 1000))
          (recur authkey meterid sdate edate compound (dec retry-n) wait-secs))
        (do
          (println "API call failed: " url)
          (System/exit 1))))))

(defn make-rows
  [body]
  (let [body (json/read-str body)]
    (mapv (fn [m]
            [(get body "meterid") (get body "compound")
             (get m "stamp") (get m "gallons") (get m "alert")])
          (get body "meterdata"))))

;; =========
;; Snowflake

(defn snowflake-conn
  [db-credentials]
  (let [conn-str (str
                   "jdbc:snowflake://"
                   (:account-name db-credentials)
                   ".snowflakecomputing.com/")
        props (doto (java.util.Properties.)
                (.put "account" (:account-name db-credentials))
                (.put "user" (db-credentials :user))
                (.put "password" (db-credentials :password))
                (.put "db" (db-credentials :dbname))
                (.put "warehouse" (:warehouse db-credentials)))]
    (try (java.sql.DriverManager/getConnection conn-str props)
         (catch java.sql.SQLException e
           (println "Failed to get SQL connection with credentials: " db-credentials)
           (println "Exception message: " (.getMessage e))
           (System/exit 2)))))

(defn q [s] (str "'" s "'"))

(defn ->timestamp
  [date]
  (let [[year month day hour minute] (s/split date #"-")]
    (str year "-" month "-" day " " hour ":" minute)))

(defn ->timestamp-q
  [date]
  (str "'" (->timestamp date) "'"))

(defn vals-str
  [[meterid compound stamp gallons alert]]
  (str "(" meterid "," (q compound) "," (q stamp) "," gallons "," (q alert) ")"))

;; todo: semicolon?
(defn insert-str
  [table rows]
  (str
    (reduce (fn [sql-str row]
              (str sql-str ", " (vals-str row)))
            (str "insert into " table " values " (vals-str (first rows)))
            (rest rows)) ";"))

(defn delete-str
  [table meterid sdate edate compound]
  (str "delete from " table " where METERID=" meterid " and STAMP between "
       (->timestamp-q sdate) " and " (->timestamp-q edate) " and COMPOUND=" (q compound) ";"))

(defn select-str
  [table [meterid compound stamp gallons alert]]
  (str "select METERID, STAMP from " table " where METERID=" meterid " and STAMP=" (q stamp) ";"))

;; =========
;; Main

(defn validate-date
  [date]
  (try (let [format (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd-hh-mm")]
         (java.time.LocalDate/parse date format)
         true)
       (catch java.time.format.DateTimeParseException e
         false)))

(defn validate-order-start-end
  [sdate edate]
  (.isBefore (java.time.LocalDate/parse sdate (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd-hh-mm"))
             (java.time.LocalDate/parse edate (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd-hh-mm"))))


(defn ->iso-local-date-format
  [date]
  (.format date DateTimeFormatter/ISO_LOCAL_DATE))

(defn ->date-time-ampm-format
  [date]
  (.format date (DateTimeFormatter/ofPattern "yyyy-MM-dd hh:mm a")))

;; https://github.com/clojure/tools.cli
(defn cli-options [today]
  [["-s" "--sdate DATE" "Earliest date of date range, ISO 8601 format, (YYYY-MM-DD-hh-mm)"
    :default (->iso-local-date-format (LocalDateTime/of today (java.time.LocalTime/MIN)))
    :parse-fn s/trim
    :validate [#(or (nil? %) (validate-date %))]]
   ["-e" "--edate DATE" "Latest date of date range, ISO 8601 format, (YYYY-MM-DD-hh-mm)"
    ;; todo: date hours: 24 hours?
    :default (->iso-local-date-format (LocalDateTime/of (.plusDays today 6) (java.time.LocalTime/MAX)))
    :parse-fn s/trim
    :validate [#(or (nil? %) (validate-date %))]]
   ["-c" "--compound Y/N" "WaterSignal to calculate compound usage before returning data, String, \"Y\" or \"N\""
    :default "N"
    :parse-fn s/trim
    :validate [#(contains? #{"Y" "N" "y" "n"} %) "Must be Y or N."]]
   ["-o" "--outfile" "Path to flat file where meter data will be written."
    :default "meterdata.txt"
    :parse-fn s/trim
    :validate [string? "Invalid outfile."]]
   ["-h" "--help"]])

(defn write-to-file
  [f rows separator]
  (let [content (map (fn [row] (str (s/join separator row) "\n")) rows)]
    (spit f content :append true)))

(defn log
  [level & args]
  (let [now (->date-time-ampm-format (LocalDateTime/now))
        level (case level
                :info "Info"
                :warn "Warn"
                :error "Error")]
    (apply println "| " now " | " level " | " args)))

(defn log-info
  [& args]
  (apply log :info args))

(defn print-options
  [options arguments]
  (println "Launched with options: ")
  (doseq [[k v] options]
    (println (name k) ":" v))
  (println "... and arguments: ")
  (println "Config path: " (first arguments)))

(defn -main [& args]
  (log-info "Start script")
  (let [t-start (LocalDateTime/now)
        {:keys [options arguments errors summary]} (parse-opts args (cli-options (LocalDate/now)))
        {:keys [sdate edate compound outfile help]} options
        config-path (first arguments)
        conf (json/read-str (slurp config-path) :key-fn keyword)
        conn (snowflake-conn (:snowflake-config conf))
        table (get-in conf [:snowflake-config :table])
        {:keys [authkey meterids]} (get conf :watersignal)]
    (when help
      (println summary)
      (System/exit 0))
    (assert (nil? errors) (str "\n" (s/join "\n" (conj errors summary))))
    (assert (or (and (nil? sdate) (nil? edate)) (and sdate edate)) "Mising start date or end date.")
    (print-options options arguments)
    (log-info "Processing " (count meterids) " meterids.")
    (jdbc/execute! conn [(str "alter warehouse " (get-in conf [:snowflake-config :warehouse]) " resume if suspended;")])
    (loop [meterids meterids]
      (if-let [meterid (first meterids)]
        (do
          (log-info "Making Watersignal API request for meterid: " meterid)
          (let [body (get-meter-data authkey meterid sdate edate compound 3 30)
                rows (make-rows body)]
            (log-info "Watersignal API returned " (count rows) " rows")
            (when-not (empty? rows)
              (log-info "Writing " (count rows) " rows to " outfile " for meterid " meterid)
              (write-to-file outfile rows " | ")
              ;; todo: execute-batch!
              ;; https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.3.847/api/next
              (log-info "Deleting rows in Snowflake...")
              ;(log-info (delete-str table meterid sdate edate compound))
              (let [r (jdbc/execute! conn [(delete-str table meterid sdate edate compound)])
                    c (get-in r [0 :next.jdbc/update-count])]
                (log-info "Deleted " c " rows from Snowflake."))
              (log-info "Writing " (count rows) " rows to Snowflake table  " table " for meterid " meterid)
              ;(log-info (insert-str table rows))
              (let [r (jdbc/execute! conn [(insert-str table rows)])
                    c (get-in r [0 :next.jdbc/update-count])]
                (log-info "Wrote " c " rows to Snowflake.")))
            (recur (rest meterids))))
        (let [t-end (LocalDateTime/now)
              elapsed (.until t-start t-end java.time.temporal.ChronoUnit/SECONDS)]
          (log-info "Finised script. Time elapsed: " elapsed " seconds."))))))

;java -jar meterdata.jar -s 2022-09-01-00-00 -e 2022-09-30-12-00 config.json
;;(-main "-s 2022-09-29-00-00" "-e 2022-09-30-12-00" "config.json")

(defn f [conn]
  (jdbc/execute! conn ["ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'"])
  (let [a (jdbc/execute! conn
                         ["select * from METER_DATA2 where METERID=88678 and STAMP between
                       '2022-09-01 00:00' and '2022-09-30 12:00' and COMPOUND='N'"])]
    (println ">>> COUNT: " (count a))
    (-main "-s 2022-09-01-00-00" "-e 2022-09-30-12-00" "config.json")))



