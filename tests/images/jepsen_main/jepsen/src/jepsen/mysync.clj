(ns jepsen.mysync
  "Tests for mysync"
  (:require [clojure.tools.logging :refer :all]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.string :as string]
            [jepsen [tests :as tests]
                    [os :as os]
                    [db :as db]
                    [client :as client]
                    [control :as control]
                    [nemesis :as nemesis]
                    [generator :as gen]
                    [checker :as checker]
                    [util :as util :refer [timeout]]
                    [net :as net]]
            [knossos [op :as op]]
            [clojure.java.jdbc :as j]
            [zookeeper :as zk]))

(def register (atom 0))

(def zk-user "testuser")
(def zk-password "testpassword123")

(defn open-conn
  "Given a JDBC connection spec, opens a new connection unless one already
  exists. JDBC represents open connections as a map with a :connection key.
  Won't open if a connection is already open."
  [spec]
  (if (:connection spec)
    spec
    (j/add-connection spec (j/get-connection spec))))

(defn close-conn
  "Given a spec with JDBC connection, closes connection and returns the spec w/o connection."
  [spec]
  (do
    (info (str "close-conn for " (:subname spec)))
    (when-let [c (:connection spec)]
      (.close c))
  )
  {:classname   (:classname spec)
   :subprotocol (:subprotocol spec)
   :subname     (:subname spec)
   :user        (:user spec)
   :password    (:password spec)})

(defmacro with-conn
  "This macro takes that atom and binds a connection for the duration of
  its body, automatically reconnecting on any
  exception."
  [[conn-sym conn-atom] & body]
  `(let [~conn-sym (locking ~conn-atom
                     (swap! ~conn-atom open-conn))]
     (try
       ~@body
       (catch Throwable t#
         (locking ~conn-atom
           (swap! ~conn-atom (comp open-conn close-conn)))
         (throw t#)))))

(defn conn-spec
  "Return mysql connection spec for given node name"
  [node]
  {:classname   "com.mysql.cj.jdbc.Driver"
   :subprotocol "mysql"
   :subname     (str "//" (name node) ":3306/test1?useSSL=false")
   :user        "client"
   :password    "client_pwd"})

(defn zk-connect-auth
  "Connect to ZooKeeper and add digest auth required to read /test paths."
  [test]
  (let [zk-hosts (clojure.string/join
                   ","
                   (map (fn [x] (str x ":2181"))
                        (filter (fn [x] (string/includes? (name x) "zookeeper"))
                                (:nodes test))))
        client   (zk/connect zk-hosts)]
    (zk/add-auth-info client "digest" (str zk-user ":" zk-password))
    client))

(defn resolve-master
  "Query ZooKeeper for the current mysync master hostname."
  [test]
  (let [client (zk-connect-auth test)]
    (try
      (clojure.string/replace
        (String. (get (zk/data client "/test/master") :data))
        #"\"" "")
      (finally (zk/close client)))))

(defn noop-client
  "Noop client"
  []
  (reify client/Client
    (setup! [_ test]
      (info "noop-client setup"))
    (invoke! [this test op]
      (assoc op :type :info, :error "noop"))
    (close! [_ test])
    (teardown! [_ test] (info "teardown"))
    client/Reusable
    (reusable? [_ test] true)))

(defn read-portion
  "Read a single chunk of values in (low, high] from connection c."
  [c low high]
  (let [q (str "select value from test1.test_set where value > " low " and value <= " high)]
    (info (str "Query: " q))
    (->> (j/query c [q] {:row-fn :value})
         (into #{}))))

(defn mysql-client
  "MySQL client"
  [conn]
  (reify client/Client
    (setup! [_ test]
      (info "mysql-client setup"))
    (open! [_ test node]
      (let [conn (atom (conn-spec node))]
        (cond (not (string/includes? (name node) "zookeeper"))
              (mysql-client conn)
              true
              (noop-client))))

    (invoke! [this test op]
      (try
            (case (:f op)
              :read
                (let [master-host (resolve-master test)
                      master-conn (atom (conn-spec master-host))]
                  (info (str "Resolved master: " master-host))
                  (try
                    (with-conn [c master-conn]
                      (let [max-value  (:max_value (first (j/query c ["select max(value) as max_value from test1.test_set"])))
                            chunk-size 10000
                            chunks     (if (or (nil? max-value) (zero? max-value))
                                         0
                                         (long (Math/ceil (/ (double max-value) chunk-size))))]
                        (info (str "max-value=" max-value " chunks=" chunks))
                        (let [result
                              (loop [n 0 acc #{}]
                                (if (>= n chunks)
                                  acc
                                  (let [low     (* n chunk-size)
                                        high    (* (inc n) chunk-size)
                                        portion (loop [attempt 0]
                                                  (let [res (try
                                                              (timeout 30000 ::chunk-timeout
                                                                       (read-portion c low high))
                                                              (catch Throwable t
                                                                (warn (str "chunk " n
                                                                           " attempt " attempt
                                                                           " error: " (.getMessage t)))
                                                                ::chunk-error))]
                                                    (if (or (= res ::chunk-timeout)
                                                            (= res ::chunk-error))
                                                      (if (< attempt 2)
                                                        (do
                                                          (locking master-conn
                                                            (swap! master-conn (comp open-conn close-conn)))
                                                          (recur (inc attempt)))
                                                        ::chunk-failed)
                                                      res)))]
                                    (if (= portion ::chunk-failed)
                                      ::abort
                                      (recur (inc n) (into acc portion))))))]
                          (if (= result ::abort)
                            (assoc op :type :info, :error "chunk-failed")
                            (do
                              (info (str "Read OK: " (count result) " elements"))
                              (assoc op :type :ok, :value result))))))
                    (finally (close-conn @master-conn))))
              ; inserts value into table
              :add (timeout 5000 (assoc op :type :info, :error "add-timeout")
                  (with-conn [c conn]
                    (do
                      (info (str "Adding: " (get op :value) " to " (get c :subname)))
                      (j/execute! c [(str "insert into test1.test_set values ('" (get op :value) "')")])
                      (assoc op :type :ok)))))
        (catch Throwable t#
          (let [m# (.getMessage t#)]
            (cond
              (re-find #"The MySQL server is running with the --(super-)?read-only option so it cannot execute this statement" m#) (assoc op :type :info, :error "catch-read-only")
              (re-find #"The server is currently in offline mode" m#) (assoc op :type :info, :error "catch-offline")
              true (do
                     (warn (str "Query error: " m# " on op: " (:f op)))
                     (assoc op :type :info, :error m#)
                    ))))))

    (close! [_ test] (close-conn conn))
    (teardown! [_ test])
    client/Reusable
    (reusable? [_ test] true)))

(defn db
  "MySQL database"
  []
  (reify db/DB
    (setup! [_ test node]
      (info (str (name node) " setup")))

    (teardown! [_ test node]
      (info (str (name node) " teardown")))))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn a [_ _] {:type :invoke, :f :add, :value (swap! register (fn [current-state] (+ current-state 1)))})

(def mysync-set
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was attempted."
  (reify checker/Checker
    (check [this test history opts]
      (let [attempts (->> history
                      (r/filter op/invoke?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            adds (->> history
                      (r/filter op/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            final-read (->> history
                      (r/filter op/ok?)
                      (r/filter #(= :read (:f %)))
                      (r/map :value)
                      (reduce (fn [_ x] x) nil))]
        (if-not final-read
          {:valid? false
           :error  "Set was never read"}

          (let [; The OK set is every read value which we tried to add
                ok          (set/intersection final-read attempts)

                ; Unexpected records are those we *never* attempted.
                unexpected  (set/difference final-read attempts)

                ; Lost records are those we definitely added but weren't read
                lost        (set/difference adds final-read)

                ; Recovered records are those where we didn't know if the add
                ; succeeded or not, but we found them in the final set.
                recovered   (set/difference ok adds)]

            {:valid?          (and (empty? lost) (empty? unexpected))
             :ok              (util/integer-interval-set-str ok)
             :lost            (util/integer-interval-set-str lost)
             :unexpected      (util/integer-interval-set-str unexpected)
             :recovered       (util/integer-interval-set-str recovered)
             :ok-frac         (util/fraction (count ok) (count attempts))
             :unexpected-frac (util/fraction (count unexpected) (count attempts))
             :lost-frac       (util/fraction (count lost) (count attempts))
             :recovered-frac  (util/fraction (count recovered) (count attempts))}))))))

(defn killer
  "Executes pkill -9 `procname`"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)
    (invoke! [this test op]
             (case (:f op)
               :kill (assoc op :value
                            (try
                              (let [procname (rand-nth [:mysqld
                                                        :mysync])
                                    node (rand-nth (filter (fn [x] (not (string/includes? (name x) "zookeeper")))
                                                           (:nodes test)))]
                                (control/on node
                                  (control/exec :pkill :-9 procname)))
                              (catch Throwable t#
                                (let [m# (.getMessage t#)]
                                  (do (warn (str "Unable to run pkill: "
                                                 m#))
                                      m#)))))))
    (teardown! [this test]
      (info (str "Stopping killer")))
    nemesis/Reflection
    (fs [this] #{})))

(defn zk-switcher
  []
  "Executes switchover"
  (reify nemesis/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
             (case (:f op)
                :switch (assoc op :value
                          (try
                            (let [client (zk-connect-auth test)]
                              (let [master (clojure.string/replace (String. (get (zk/data client "/test/master") :data))
                                                                   #"\"" "")
                                    node (rand-nth (filter (fn [x] (not (string/includes? (name x) "zookeeper"))) (:nodes test)))]
                                 (info (str "running switchover from " master))
                                 (zk/close client)
                                 (control/on node
                                   (control/exec :mysync :switch :--from master))))
                             (catch Throwable t#
                               (let [m# (.getMessage t#)]
                                 (do (warn (str "Unable to run switch: "
                                                m#))
                                    m#)))))))
    (teardown! [this test]
      (info (str "Stopping switcher")))
    nemesis/Reflection
    (fs [this] #{})))

(def nemesis-starts [:start-halves :start-ring :start-one :switch :kill])

(defn mysync-test
  [mysql-nodes zookeeper-nodes]
  {:nodes     (concat mysql-nodes zookeeper-nodes)
   :name      "mysync"
   :os        os/noop
   :db        (db)
   :ssh       {:private-key-path "/root/.ssh/id_rsa" :strict-host-key-checking :no}
   :net       net/iptables
   :client    (mysql-client nil)
   :nemesis   (nemesis/compose {{:start-halves :start} (nemesis/partition-random-halves)
                                {:start-ring   :start} (nemesis/partition-majorities-ring)
                                {:start-one    :start
                                 ; All partitioners heal all nodes on stop so we define stop once
                                 :stop         :stop} (nemesis/partition-random-node)
                                #{:switch} (zk-switcher)
                                #{:kill} (killer)})
   :generator (gen/phases
                (->> a
                     ; generate write requests 50 per sec for 3600 seconds
                     (gen/stagger 1/50)
                     (gen/nemesis
                       ; we cycle through these steps for 1 hour (3600 seconds):
                       ; * take 2 random nemesis
                       ; * sleep for 60 seconds
                       ; * stop nemesis
                       ; * sleep for 60 seconds
                       ; repeat
                       (fn [] (map gen/once
                                   [{:type :info, :f (rand-nth nemesis-starts)}
                                    {:type :info, :f (rand-nth nemesis-starts)}
                                    {:type :sleep, :value 60}
                                    {:type :info, :f :stop}
                                    {:type :sleep, :value 60}])))
                      (gen/time-limit 3600))
                 (gen/sleep 10)
                 (->> r
                      (gen/stagger 30)
                      (gen/nemesis
                        (fn [] (map gen/once
                                    [{:type :info, :f :stop}
                                     {:type :sleep, :value 300}])))
                      (gen/time-limit 600)))
   :checker   mysync-set
   :remote    control/ssh})
