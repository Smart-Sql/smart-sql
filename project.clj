(defproject smart-sql "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.1.587"]
                 [org.clojure/core.specs.alpha "0.2.44" :exclusions [[org.clojure/clojure] [org.clojure/spec.alpha]]]
                 [org.clojure/spec.alpha "0.2.176" :exclusions [[org.clojure/clojure]]]
                 [org.gridgain/ignite-core "8.7.24"]
                 [org.gridgain/ignite-indexing "8.7.24"]
                 [org.gridgain/ignite-spring "8.7.24"]
                 [org.gridgain/ignite-h2 "8.7.24"]
                 [org.gridgain/ignite-schedule "8.7.24"]
                 [cn.mysuper/my-super-service "1.0-SNAPSHOT"]
                 [org.springframework/spring-core "4.3.25.RELEASE"]
                 [org.springframework/spring-aop "4.3.25.RELEASE"]
                 [org.springframework/spring-beans "4.3.25.RELEASE"]
                 [org.springframework/spring-context "4.3.25.RELEASE"]
                 [org.springframework/spring-expression "4.3.25.RELEASE"]
                 [org.springframework/spring-tx "4.3.25.RELEASE"]
                 [org.springframework/spring-jdbc "4.3.25.RELEASE"]
                 [commons-logging/commons-logging "1.1.1"]
                 [junit/junit "4.11"]]
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :test-paths ["test/main/clojure" "test/main/java"]
  :resource-paths ["resources"]
  :aot :all
  :repl-options {:init-ns core})
