(ns lucene-clojure.core
  (:gen-class)
  (:import [org.apache.lucene.analysis Analyzer]
           [org.apache.lucene.analysis.custom CustomAnalyzer CustomAnalyzer$Builder]
           [org.apache.lucene.analysis.standard StandardAnalyzer StandardTokenizerFactory]
           [org.apache.lucene.analysis.core LowerCaseFilterFactory StopFilterFactory]
           [org.apache.lucene.analysis.en EnglishPossessiveFilterFactory PorterStemFilterFactory]
           [org.apache.lucene.analysis.miscellaneous KeywordMarkerFilterFactory]
           [org.apache.lucene.index IndexWriter IndexWriterConfig IndexReader DirectoryReader]
           [org.apache.lucene.store MMapDirectory Directory]
           [java.nio.file Files Path Paths]
           [java.nio.file.attribute FileAttribute]
           [org.apache.lucene.document Document TextField Field$Store]
           [org.apache.lucene.search IndexSearcher TopDocs ScoreDoc Query]
           [org.apache.lucene.queryparser.classic QueryParser QueryParser$Operator])
  (:require [cheshire.core :refer :all]
            [clj-http.client :as client]))

(defn- get-release-content
  "Parses GNW json feed to generate content"
  []
  (def url "https://globenewswire.com/JsonFeed/Content/FullText/max/1")
  (as-> url x
        (client/get x)
        (:body x)
        (parse-string x true)
        (first x)
        ((juxt :Title :Content) x)
        (flatten x)
        (interpose " " x)
        (apply str x)))

(defn- get-subscription_keywords
  "Reads a list of subscription keywords from text file"
  []
  (clojure.string/split-lines (slurp "conf/subscription_keywords.txt")))

(defn -main
  "Performing a Lucene search"
  [& args]

  (println "starting...")
  ;;(def ^Analyzer analyzer (StandardAnalyzer.))

  (def ^CustomAnalyzer$Builder builder
    (doto
      (CustomAnalyzer/builder (Paths/get "conf" (make-array String 0)))
      (.withTokenizer StandardTokenizerFactory/NAME (make-array String 0))
      (.addTokenFilter StopFilterFactory/NAME (into-array ["ignoreCase" "true", "words" "stopwords.txt" "format" "wordset"]))
      (.addTokenFilter LowerCaseFilterFactory/NAME (make-array String 0))
      (.addTokenFilter EnglishPossessiveFilterFactory/NAME  (make-array String 0))
      (.addTokenFilter KeywordMarkerFilterFactory/NAME (into-array ["protected" "protwords.txt"]))
      (.addTokenFilter PorterStemFilterFactory/NAME (make-array String 0))))

  (def ^Analyzer analyzer (.build builder))

  (def ^Path path (Files/createTempDirectory "mmap" (make-array FileAttribute 0)))
  (def ^Directory directory (MMapDirectory. (.toAbsolutePath path)))
  (def config (IndexWriterConfig. analyzer))
  (def indexWriter (IndexWriter. directory config))
  (def document (Document.))
  (def content (get-release-content))
  (.add document (TextField.  "text_all" content Field$Store/YES))
  (.addDocument indexWriter document)
  (.close indexWriter)
  (def ^IndexReader indexReader (DirectoryReader/open directory))
  (def indexSearcher (IndexSearcher. indexReader))
  (def parser (QueryParser. "text_all" analyzer))
  (.setDefaultOperator parser QueryParser$Operator/AND)
  (def hitsPerPage 10)
  (doseq [^String keyword (get-subscription_keywords)]
    (let [^Query query (.parse parser (QueryParser/escape keyword))
          ^TopDocs docs (.search indexSearcher query hitsPerPage)
          #^ScoreDoc hits (.scoreDocs docs) ;; hits is Java array of ScoreDoc[]
          end (min (.-value (.totalHits docs)) hitsPerPage)
          text_all
          (for [^ScoreDoc hit (map (partial aget hits) (range end))]
            (let [doc-id (.doc hit)
                  ^Document d (.doc indexSearcher doc-id)]
              (.get d "text_all")))]
      (print (if (zero? end) "" (str keyword "\n") ))))
  (println "-----------------------------\n" content))