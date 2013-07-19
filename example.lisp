(in-package #:cl-db)

(define-configuration (projects test-mapping)
    ((:default t)
     (:update-schema t))
  (:open-connection #'(lambda ()
			(cl-postgres:open-database "projects"
						   "makarov" "zxcvb"
						   "localhost")))
  (:close-connection #'cl-postgres:close-database)
  (:prepare #'cl-postgres:prepare-query)
  (:execute #'(lambda (connection name &rest params)
	      (cl-postgres:exec-prepared connection name params
					 cl-postgres:alist-row-reader)))
  (:list-metadata #'cl-db:list-postgresql-metadata))

(with-session () ;; or (with-session (projects)
  (db-read :all 'project-manager
	   :where #'(lambda (manager)
		      (eq (login-of manager) "makarov"))
	   :also-fetch #'project-of
	   :order-by #'name-of))

;;(list (where (query-over 'project)
;;       #'(lambda (project)
;;	   (eq project 1))))
;;		       
;;		   (
;;	    :where (and-expression )

;; сделать db-read макросом? или оставить как функцию, а переделывать выражение в with-session?
;; осталось написать

;;(make-instance 'registry
;;	       :mappers (list (make-instance 'mapper :class (find-class 'project)
;;					     :select-all '(:select id name :from projects)
;;					     :select

;;(defvar *session*)

;;(defun db-read (&key all)
;;  (db-find-all all))

;;(defun db-find-all (session class-name)
;;  (let ((class-mapping (gethash class-name (mappings-of session))))
;;    (map 'list #'(lambda (row)
;;		   (load row class-mapping session))
;;	 (execute (connection-of session)
;;		  (make-select-query (table-of class-mapping))))))

;;(defun make-select-query (table)
;;  (let ((table-name (name-of table)))
;;    (format nil "SELECT ~{~a~^, ~} FROM ~a"
;;	    (map 'list #'(lambda (column)
;;			   (format nil "~a.~a" table-name (name-of column)))
;;		 (columns-of table)) table-name)))

;;(defclass clos-transaction ()
;;  ((clos-session :initarg :clos-session :reader clos-session-of)
;;   (new-objects :initform (list) :accessor new-objects-of)
;;   (objects-snapshots :initarg :objects-snapshots :reader object-snapshots)))

;;(defun begin-transaction (&optional (session *session*))
;;  (make-instance 'clos-transaction :clos-session clos-session
;;		 :objects-snapshots (make-snapshot (list-loaded-objects session)
;;						   (mappings-of session))))

;;(defun list-loaded-objects (session)
  

;;(defun persist (object &optional (transaction *transaction*))
;;  (when (not (loaded-p object (clos-session-of transaction)))
;;    (push (new-objects-of transaction) object)))



;;(defgeneric db-query (connection query))

;;(defun load (mapping row))

;;  (query-db (connection-of session)
;;	    (get-mapping class-name)

;;(defun db-get (class-name &rest primary-key)

;;

;;(with-session (*session*)
;;  (db-read :all 'project))





;;(defun db-find-all (session class &rest fetch) 
;;  (let* ((class-mapping (get-class-mapping session class))
;;	 (result (execute (make-select-query class-mapping fetch)
;;			  (connection-of session))))
;;    (map 'list #'(lambda (table-row)
;;		   (load session class-mapping fetch table-row))
;;	 table-rows)))

;;(defun db-read (&key all)
;;  (db-find-all *session* (find-class all)))

(defclass table-expression ()
  ((table-reference :initarg :table-reference
		    :reader table-reference-of)
   (joins :initarg :joins
	  :reader joins-of)))

(defclass join (table-expression)
  ((column-pairs :initarg :column-pairs
		 :reader column-pairs-of)))

(defclass inner-join (join) ())

(defclass left-outer-join (join) ())

(defclass select-query ()
  ((table-expression :initarg :table-expression
		     :reader table-expression-of)
   (select-list-items :initarg :select-list-items
		      :reader select-list-items-of)))

(defun compute-column-pairs (table foreign-key)
  (mapcar #'cons
	  (primary-key-of table)
	  (alexandria:hash-table-keys (columns-of foreign-key))))

(defun compute-superclasses-joins (table-reference superclass-mappings)
  (let ((table (table-of table-reference)))
    (mapcar #'(lambda (superclass-mapping)
		(let* ((superclass-table (table-of superclass-mapping))
		       (superclass-table-reference (make-instance 'table-reference
								  :table superclass-table)))
		  (cons (make-instance 'inner-join
				       :table-reference table-reference
				       :joined-table-reference superclass-table-reference
				       :column-pairs (compute-columns-pairs superclass-table
									    (get-foreign-key table
											     (primary-key-of table)
											     superclass-table)))
			(compute-joins superclass-table-reference superclass-mapping))))
	    superclass-mappings)))

(defun compute-subclasses-joins (table-reference subclasses-mappings)
  (let ((table (table-of table-reference)))
    (mapcar #'(lambda (subclass-mapping)
		(let* ((subclass-table (table-of subclass-mapping))
		       (subclass-table-reference (make-instance 'table-reference
								:table subclass-table)))
		  (cons (make-instance 'left-outer-join
				       :table-reference table-reference
				       :joined-table-reference subclass-table-reference
				       :column-pairs (compute-columns-pairs subclass-table
									    (get-foreign-key subclass-table
											     (primary-key-of subclass-table)
											     table)))
			(compute-class-joins superclass-mapping))))
	    subclasses-mappings)))

(defun compute-fetch-joins (class-mapping joined-fetch)
  (mapcar #'(lambda (joined-fetch)
	      (make-instance 'left-outer-join
			     :foreign-key (foreign-key-of
					   (get-reference-mapping class-mapping
								  joined-fetch))))
	  joined-fetch))

(defclass joined-fetch ()
  ((reference-reader :initarg :reference-reader
		     :reader reference-reder-of)
   (joined-fetch :initarg :joined-fetch
		 :reader joine-fetch-of)))

(defun compute-joins (table-reference class-mapping)
  (append
   (compute-subclasses-joins table-reference
			     (superclasses-mappings-of class-mapping))
   (compute-superclasses-joins table-reference
			       (subclasses-mappings-of class-mapping))))



(defgeneric make-query-string (query))

;;(defmethod make-query-string ((query select-query))
;;  (format nil "SELECT ~{~a~^, ~} FROM ~a 

;; execute

;;defclass result - hash-table of table-rows on foreign0keys
;; primary-key inheritance
;;(defclass db-query ()
;;  ((table :initarg :table :reader table)
;;   (foreign-keys :initarg :foreign-keys :reader foreign-keys)))

;;(defclass table-row ()
;;  ((values :initarg :values
;;	   :reader values-of)
;;   (foreign-key-tables-rows :initarg :foreign-key-tables-rows
;;			    :reader foreign-key-tables-rows-of)))

;; load

;; наследование и инициализация слотов
;; объект какого класса создавать? При условии, что нет абстрактных классов
;; объекты по иерархии загружаются снизу-вверх из исключенных записей нижнего уровня


;; скорее всего функция будет рекурсивной (особенно если fetch будет с Join'ами)
;;(defun load (session class-mapping fetch result)
;;  (let ((object (allocate-instance (mapped-class-of class-mapping))))
;;    (cache object session)
;;    (maphash #'(lambda (slot-name slot-mapping)
;;		 (

(lift:addtest (test-mappings) object-loaders
	      (with-session (session)
		(assert (= (length
			    (subclass-object-loaders-of
			     (make-instance 'object-loader
					    :class-mapping (get-class-mapping session
									      (find-class 'project)))))
			   0))
		(assert (= (length
			    (subclass-object-loaders-of
			     (make-instance 'object-loader
					    :class-mapping (get-class-mapping session
									      (find-class 'project-member)))))
			   1))))

(lift:addtest (test-mappings) making-simple-select
	      (with-session (session)
		(assert
		 (string= (make-query-string
			   (make-select-query
			    (get-class-mapping session (find-class 'project))))
			  "SELECT t_1.id, t_1.name, t_1.begin_date FROM projects as t_1"))))


(lift:addtest (test-mappings) making-select-with-inheritance
	      (with-session (session)
		(assert
		 (string= (make-query-string
			   (make-select-query
			    (get-class-mapping session (find-class 'project-manager))))
			  "SELECT t_1.user_id, t_1.project_id, t_2.user_id, t_2.project_id FROM project_members as t_1 INNER JOIN project_managers as t_2 ON t_1.user_id = t_2.user_id AND t_1.project_id = t_2.project_id"))))

;;;;;;;;;;;;;;;;;;


(define-class-mapping organization ("organizations" "id")
  (id (value ("id" "integer")))
  (name (value ("name" "varchar"))))

(define-class-mapping user ("users" "id")
  (id (value ("id" "integer")))
  (name (value ("name" "varchar")))
  (login (value ("login" "varchar")))
  (password (value ("password" "varchar")))
  (project-managments (one-to-many project-managment ("project_id"))
		      #'(lambda (&rest roles)
			  (alexandria:alist-hash-table
			   (mapcar #'(lambda (role)
				       (cons (project-of role) role))
				   roles)))
		      #'alexandria:hash-table-values))

(define-class-mapping project ("projects" "id")
  (id (value ("id" "integer")))
  (name (value ("name" "varchar")))
  (begin-date (value ("begin_date" "date")))
  (main-plan (many-to-one project-plan "main_plan_id"))
  (project-plans (one-to-many project-plan "project_id"))
  (project-members (one-to-many project-member ("project_id"))
		   #'(lambda (&rest roles)
		       (alexandria:alist-hash-table
			(mapcar #'(lambda (role)
				    (cons (user-of role) role))
				roles)))
		   #'alexandria:hash-table-values)
  (tasks (one-to-many task "project_id")))

(define-class-mapping project-task
    ("project_tasks" "project_id" "id")
  (id (value ("id" "integer")))
  (project (many-to-one project "project_id"))
  (name (value ("name" "varchar")))
  (description (value ("description" "varchar"))))

(define-class-mapping project-participation
    ("project_memebers" "project_id" "user_id")
  (project (many-to-one project "project_id"))
  (user (many-to-one user "user_id")))

(define-class-mapping project-managment
    ("project_managers"
     (project-participation "project_id" "user_id")))

(define-class-mapping project-plan
    ("project_plans" "project_id" "id")
  (id (value ("id" "integer")))
  (project (many-to-one project "project_id"))
  (name (value ("name" "varchar")))
  (user (many-to-one user "user_id"))
  (tasks
   (many-to-one tasks-alteration "tasks_alteration_id"))
  (tasks-alterations
   (one-to-many tasks-alteration "project_id" "plan_id")))



(define-class-mapping tasks-alteration
    ("tasks_alterations" "project_id" "id")
  (id (value ("id" "integer")))
  (project (many-to-one project "project_id"))
  (timestamp (value "date" "timestamp"))
  (task-list-tree-node
   (many-to-one task-list-tree-node
		"project_id" "tasks_alteration_id")))

(define-class-mapping task-list-tree-node
    ("task-list-tree-node" "project_id" "id")
  (id (value ("id" "integer")))
  (project (many-to-one project "project_id"))
  (key (value ("key" "integer")))
  (value (many-to-one project-task "task_id"))
  (left (many-to-one (or task-list-tree-node null)
		     "project_id" "left_node_id"))
  (right (many-to-one (or task-list-tree-node null)
		      "project_id" "right_node_id")))

(define-class-mapping plan-object ("plan_objects" "project_id" "id")
  (id (value ("id" "integer")))
  (project (many-to-one project "project_id"))
  (name (value ("name" "varchar")))
  (description (value ("description" "varchar")))
  (child-objects (one-to-many plan-subobject "project_id" "plan_id")))

(define-class-mapping plan-subobject ("plan_subobjects" plan-object)
  (parent-object (many-to-one plan-object "project_id" "id")))

;;(define-subclass-mapping project-managment
;;    ("project_managers"
;;     (project "id")
;;     (project-participation "project_id" "user_id")))

