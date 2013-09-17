(in-package #:cl-db)

(defclass cat ()
  ((id :initarg :id
       :reader id-of)
   (age :initarg :age
	:accessor age-of)
   (name :initarg :name
	 :accessor name-of)
   (kittens :initarg :kittens
	    :accessor kittens-of)))

(defclass cat-b (cat)
  ())

(define-database-interface postgresql-postmodern
  (:open-connection #'cl-postgres:open-database)
  (:close-connection #'cl-postgres:close-database)
  (:prepare #'cl-postgres:prepare-query)
  (:execute
   #'(lambda (connection name &rest params)
       (cl-postgres:exec-prepared connection name params
				  'cl-postgres:alist-row-reader))))

(define-mapping-schema cats-mapping)

(define-class-mapping (cat "cats")
    ((:primary-key "id"))
  (id (:value ("id" "integer")))
  (name (:value ("name" "varchar")))
  (age (:value ("age" "integer")))
  (kittens (:one-to-many cat "parent_id")))

(define-class-mapping (cat-b "cats_b")
    ((:superclasses cat)))

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
   (*mapping-schema* (make-mapping-schema 'cats-mapping))))

(lift:addtest entities
  (let ((cat (bind-root 'cat)))
    (make-query cat)))

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
  ((mapping-shema (test-mapping)))
  (:test (lift:ensure (bind-root 'project mapping-schema)))
  (:test (bind-reference (bind-root 'project) #'project-members-of)))
