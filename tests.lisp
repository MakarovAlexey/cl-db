(in-package #:cl-db)

(defclass user ()
  ((name :initarg :name
	 :accessor name-of)
   (login :initarg :login
	  :accessor login-of)
   (password :initarg :password
	     :accessor password-of)
   (email :initarg :email
	  :accessor email-of)
   (project-managments :initarg :project-managments
		       :accessor project-managments-of)
   (project-project-participations :initarg :project-managments
				   :accessor project-managments-of))
  (:documentation "Пользователь системы, ответственный исполнитель"))

(defclass project ()
  ((name :initarg :name
	 :accessor name-of)
   (begin-date :initarg :begin-date
	       :accessor begin-date-of)
   (project-participations :initarg :project-members
			   :accessor project-members-of)))

(defclass project-participation ()
  ((project :initarg :project :reader project-of)
   (user :initarg :user :reader user-of)))

(defclass project-managment (project-participation)
  ())

(define-database-interface postgresql-postmodern
  (:open-connection #'cl-postgres:open-database)
  (:close-connection #'cl-postgres:close-database)
  (:prepare #'cl-postgres:prepare-query)
  (:execute
   #'(lambda (connection name &rest params)
       (cl-postgres:exec-prepared connection name params
				  'cl-postgres:alist-row-reader))))

(define-mapping-schema projects-managment)

(define-class-mapping (user "users")
    ((:primary-key "id"))
  (id (:value ("id" "serial")))
  (name (:value ("name" "varchar")))
  (login (:value ("login" "varchar")))
  (password (:value ("password" "varchar")))
  (project-managments (:one-to-many project-managment "project_id")
		      #'(lambda (&rest roles)
			  (alexandria:alist-hash-table
			   (mapcar #'(lambda (role)
				       (cons (project-of role) role))
				   roles)))
		      #'alexandria:hash-table-values))

(define-class-mapping (project "projects")
    ((:primary-key "id"))
  (id (:value ("id" "serial")))
  (name (:value ("name" "varchar")))
  (begin-date (:value ("begin_date" "date")))
  (project-participations (:one-to-many project-participation "project_id")
			  #'(lambda (&rest roles)
			      (alexandria:alist-hash-table
			       (mapcar #'(lambda (role)
					   (cons (user-of role) role))
				       roles)))
			  #'alexandria:hash-table-values))

(define-class-mapping (project-participation "project_participation")
    ((:primary-key "project_id" "user_id"))
  (project (:many-to-one project "project_id"))
  (user (:many-to-one user "user_id")))

(define-class-mapping (project-managment "project_managers")
    ((:superclasses project-participation)))

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