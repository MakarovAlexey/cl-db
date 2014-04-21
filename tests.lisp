(in-package #:cl-db)

(defclass test-class-1 ()
  ((slot-1 :accessor slot-1-of)
   (slot-2 :accessor slot-2-of)))

(defclass test-class-2 ()
  ((slot-3 :accessor slot-3-of)
   (slot-4 :accessor slot-4-of)))

(defclass test-class-3 (test-class-1 test-class-2)
  ((slot-5 :accessor slot-5-of)))

(defclass test-class-4 (test-class-3)
  ((slot-7 :accessor slot-7-of)
   (slot-8 :accessor slot-8-of)))

(defclass test-class-5-1 ()
  ((slot-9 :accessor slot-9-of)
   (slot-10 :accessor slot-10-of)))

(defclass test-class-5 (test-class-5-1)
  ((slot-11 :accessor slot-11-of)))

(defclass test-class-5-2 (test-class-5)
  ((slot-12 :accessor slot-12-of)))

(define-database-interface postgresql-postmodern
  (:open-connection #'cl-postgres:open-database)
  (:close-connection #'cl-postgres:close-database)
  (:prepare #'cl-postgres:prepare-query)
  (:execute
   #'(lambda (connection name &rest params)
       (cl-postgres:exec-prepared connection name params
				  'cl-postgres:alist-row-reader))))

(define-mapping-schema test-schema)

(define-class-mapping (test-class-1 "test_class_1")
    ((:primary-key "test_class_1_id"))
  (slot-1 (:value ("test_class_1_id" "integer")))
  (slot-2 (:one-to-many test-class-5 "reference_2_class_id")))

(define-class-mapping (test-class-2 "test_class_2")
    ((:primary-key "test_class_2_id"))
  (slot-3 (:value ("test_class_2_id" "integer")))
  (slot-4 (:many-to-one test-class-5 "reference_2_class_id")))

(define-class-mapping (test-class-3 "test_class_3")
    ((:superclasses test-class-1 test-class-2))
  (slot-5 (:value ("slot_5" "integer"))))

(define-class-mapping (test-class-4 "test_class_4")
    ((:superclasses test-class-3))
  (slot-7 (:value ("slot_7" "serial")))
  (slot-8 (:value ("slot_8" "integer"))))

(define-class-mapping (test-class-5-1 "test_class_5_1")
    ((:primary-key "id"))
  (slot-9 (:value ("id" "serial")))
  (slot-10 (:value ("slot_10" "string"))))

(define-class-mapping (test-class-5 "test_class_5")
    ((:superclasses test-class-5-1))
  (slot-11 (:value ("slot_9" "serial"))))

(define-class-mapping (test-class-5-2 "test_class_5")
    ((:superclasses test-class-5))
  (slot-12 (:value ("slot_9" "serial"))))

(lift:deftestsuite session ()
  ())

(lift:addtest session-open-and-close
  (lift:ensure-no-warning
    (with-session ((:mapping-schema cats-mapping)
		   (:database-interface postgresql-postmodern)
		   (:connection-args "projects" "makarov"
				     "zxcvb" "localhost")))))

(lift:deftestsuite query ()
  ()
  (:dynamic-variables
   (*mapping-schema* (make-mapping-schema 'projects-managment))))

(lift:addtest list-projects
  (let ((project (bind-root 'project)))
    (make-query (list project))))

(lift:addtest list-project-managers
  (let ((project-member (bind-root 'project-manager)))
    (make-query (list project-manager))))

(lift:addtest list-project-participations
  (let ((project-participation (bind-root 'project-participation)))
    (make-query (list project-participation))))

(lift:addtest join-tree-root-with-inheritance
  (destructuring-bind ((root-binding (inheritance-mapping)))
      (let ((root (bind-root 'project-managment)))
	(plan-select-item nil root))
    (lift:ensure (typep root-binding 'root-binding))
    (lift:ensure (typep inheritance-mapping 'inheritance-mapping))))

(lift:addtest join-tree-root-with-extention
  (let ((root (bind-root 'project-managment)))
    (plan-select-item nil root)))

(lift:addtest join-tree-reference-with-inheritance
  (let ((root (bind-root 'user))
	(reference (bind-reference root #'project-managments-of)))
    (plan-select-item nil reference)))

(lift:addtest join-tree-reference-with-extension
  (let ((root (bind-root 'project))
	(reference (bind-reference root #'project-members-of)))
    (plan-select-item nil reference)))
 

;; additional restrictions

(lift:addtest entities-with-name
  (let ((cat (bind-root 'cat)))
    (make-query cat :where
		(expression := (access-value cat #'name-of) "Max"))))

(lift:addtest entities-with-age-between
  (let* ((cat (bind-root 'cat))
	 (name (access-value cat #'name-of)))
    (make-query cat :select name
		:order-by (asc name)
		:where (expression :between
				   (access-value cat #'age-of) 2 8))))

(lift:addtest entities-with-age-and-name
  (let ((cat (bind-root 'cat)))
    (make-query cat :where
		(list
		 (expression := (access-value cat #'name-of) "Max")
		 (expression :between (access-value cat #'age-of) 2 8)))))

(lift:addtest entity-by-reference-name
  (let* ((cat (bind-root 'cat))
	 (kitten (bind-reference cat #'kittens-of)))
    (make-query cat
		:where (expression := (slot-of #'name-of) "Tiddles"))))

(lift:addtest entity-by-reference-with-additionl-restrictions
  (let* ((cat (bind-root 'cat))
	 (kitten (bind-reference cat #'kitten-of)))
    (make-query cat :where
		(list
		 (expression := (value-access-of cat #'age-of) 5)
		 (expression := (value-access-of kitten #'name-of)
			     "Tiddles")))))

(lift:addtest entity-values
  (let ((cat (bind-root 'cat)))
    (make-query (list (value-access-of cat #'name-of)
		      (value-access-of cat #'age-of)))))

(lift:addtest entity-value-and-agregate-function
  (let ((cat (bind-root 'cat)))
    (make-query (list
		 (value-access-of #'name-of cats)
		 (aggregate :avg (value-access-of cat #'age-of))))))

(lift:addtest entity-by-maximum-slot-value
  (let ((cat (make-bind 'cat))
	(maximum-age-cat (bind-root 'cat)))
    (make-query cat :having
		(expression := (access-value cat #'age-of)
			    (expression :max
					(access-value maximum-age-cat
						      #'age-of))))))

(lift:addtest limit-and-offset
  (make-query (bind-root 'cat)
	      :limit 100
	      :offset 100))

;; Fetching

(lift:addtest fetching
  (let ((cat (bind-root 'cat)))
    (make-query cat :fetch-also (list (fetch cat #'kittens-of)))))

;; Single instance

(lift:addtest single-entity
  (let ((cat (bind-root 'cat)))
    (make-query cat :single t :where
		(list (expression :eq (value-access #'id-of cat) 35)))))

;; Проблема рекурсивных ключей. Идентификатор объекта не может
;; идентифицироваться в дереве своим родителем. В противном случае
;; необходимо рекурсиное построение запроса.

;; If PK (user id and project id)

;; биарные операции применимы только для значений схожего типа
;; унарные применимы только для одного значения или выражения
;; таким образом возможно построение совокупности операций для стобцов

(lift:addtest query-building
  (lift:ensure-no-warning
   (let ((cat (bind-root 'cat)))
     (make-query cat))))

(lift:deftestsuite compilation ()
  ())

(lift:addtest projects-table-columns
  (lift:ensure-same
   (get-columns (find-class-mapping 'project))
   '(("id" "integer") ("name" "varchar") ("begin_date" "timestamp"))
   :test #'equal))

(lift:addtest users-table-columns
  (lift:ensure-same
   (get-columns (find-class-mapping 'user))
   '(("id" "integer") ("name" "varchar")
     ("login" "varchar") ("password" "varchar"))
   :test #'equal))

(lift:addtest project-members-table-columns
  (lift:ensure-same
   (get-columns (find-class-mapping 'project-member))
   '(("project_id" "integer") ("user_id" "integer"))
   :test #'equal))

(lift:addtest project-managers-table-columns
  (lift:ensure-same
   (get-columns (find-class-mapping 'project-manager))
   '(("user_id" "integer") ("project_id" "integer"))
   :test #'equal))

(lift:addtest reference-foreign-key-count
  (lift:ensure-same
   (length (apply #'compile-reference-foreign-keys
		  (list-class-mappings)))
   3))

(lift:addtest getting-reference
  (lift:ensure
   (bind-reference (bind-root 'project) #'project-members-of)))

(lift:deftestsuite query ()
  ((*mapping-shema* (test-mapping)))
  (:test (lift:ensure (bind-root 'project mapping-schema)))
  (:test (bind-reference (bind-root 'project) #'project-members-of))
  (:test (lift:ensure (equal
		       (ensure-node (list '(1 (2)) '(2))
				    1 2 3 4)
		       '((1 (2 (3 (4)))) (2))))))
