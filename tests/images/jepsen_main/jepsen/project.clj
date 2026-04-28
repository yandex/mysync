(defproject jepsen.mysync "0.1.0-SNAPSHOT"
  :description "mysync tests"
  :url "https://yandex.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [jepsen "0.3.11"]
                 [zookeeper-clj "0.13.0" :exclusions [org.slf4j/slf4j-api]]
                 [org.clojure/java.jdbc "0.7.12" :exclusions [org.slf4j/slf4j-api]]
                 [com.mysql/mysql-connector-j "8.4.0"]])
