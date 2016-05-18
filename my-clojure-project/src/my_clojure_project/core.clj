(comment
  This project project is an implimentation of a merge sort.
  This project is also designed to run parallel threads in 2, 4, 8, 16, 32. 
)

(ns my-clojure-project.core)

(import '(java.util.concurrent Executors))

(set! *unchecked-math* true)

;;https://gist.github.com/alco/2135276 4/29/2016
(defn merge [left right]
    (loop [l left, r right, result []] ;;recursively binds expression for each instance
      (let [lhead (first l), rhead (first r)]
        (cond ;;checks head of each list and determins which order to put in
          (nil? lhead)     (concat result r)
          (nil? rhead)     (concat result l)
          (<= lhead rhead) (recur (rest l) r (conj result lhead))
          true             (recur l (rest r) (conj result rhead))))))

(defn merge-sort [list] ;;original merge sort call 
  ((fn merge-sort-counted [list n]
     (if (<= n 1)
       list
       (let [middle (bit-shift-right n 1)]  
         (merge 
         (map merge-sort-counted 
              (split-at middle list) [middle (- n middle)]))))) 
  list (count list)))

;;http://stackoverflow.com/questions/36731188/multithreaded-merge-sort-algorithm-in-clojure 4/28/2016
(defn merge-sort-prime [list]
  (if (< (count list) 2) ;;if list is only 2 returns list
    list
    (apply merge ;;recursevly calls fuction to put in split in half list
           (map merge-sort-prime
                 (split-at (/ (count list) 2) list)))))

(defn merge-sort-2 [list] ;;Call for 2 threads to be executed
  (if (< (count list) 2)
    list
    (apply merge
           (pmap merge-sort-prime
                 (split-at (/ (count list) 2) list)))))

(defn merge-sort-4 [list] ;;Call for 4 threads to be executed
  (if (< (count list) 2)
    list
    (apply merge
           (pmap merge-sort-2
                 (split-at (/ (count list) 2) list)))))

(defn merge-sort-8 [list] ;;Call for 8 threads to be executed
  (if (< (count list) 2)
    list
    (apply merge
           (pmap merge-sort-4
                 (split-at (/ (count list) 2) list)))))

(defn merge-sort-16 [list] ;;Call for 16 threads to be executed
  (if (< (count list) 2)
    list
    (apply merge
           (pmap merge-sort-8
                 (split-at (/ (count list) 2) list)))))

(defn merge-sort-32 [list] ;;Call for 32 threads to be executed
  (if (< (count list) 2)
    list
    (apply merge
           (pmap merge-sort-16
                 (split-at (/ (count list) 2) list)))))

;;https://clojuredocs.org/clojure.java.io/reader 04/23/2016
(defn get-list [fname] (long-array ;;Reads file and converts to an array of longs
  (with-open [rdr (clojure.java.io/reader fname)] 
    (into-array (map #(Integer/parseInt %) (line-seq rdr))))))

(comment 
;;Run line
(let [list (get-list "resources/numbers.dat")] (dotimes [i 5] (time (merge-sort-prime list))))
)