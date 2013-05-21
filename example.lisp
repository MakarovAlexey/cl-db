(in-package #:cl-db)

;;(defun map-slot (slot-name place reader writer)
;;#'to-list
;;#'from-list
;; если надо достать список простых значений, то (map-table "table" "one" "two" "three") вконструктор будет переданы  значения колонок
 ;; символ - класс или функция с аргументами-значениями столбцов таблицы
;;  (many-to-many 'project-manager "table" "one" "two" "three");; инициализация значения и отображениена столбцы ;; тоже самое

;;(key-column 'id "key_id")
;;(key-many-to-one 'project 'project
;;		 (key-column "project_id")
;;		 (key-many-to-one 'ddd 'ddd "ddd_id"))

;;(defun simple-value (column)
;;  (apply #'value #'simple #'simple column))

;; способ инициализации слота, initarg или setf? 
(map-class 'user :table-name "users" :primary-key '(id)
	   :slots (list (map-slot 'id (value "id"))
			(map-slot 'name (value "name"))
			(map-slot 'login (value "login"))
			(map-slot 'password (value "password"))
			(map-slot 'managed-projects
				  (one-to-many 'project-manager 'user)
				  #'(lambda (&rest roles)
				      (reduce #'(lambda (table role)
						  (setf (gethash (project-of role) table) role)
						  table)
					      roles
					      :initial-value (make-hash-table :size (length roles))))
				  #'alexandria:hash-table-values)))

(map-class 'project :table-name "projects" :primary-key '(id)
	   :slots (list (map-slot 'id (value "id"))
			(map-slot 'name (value 'name "name"))
			(map-slot 'begin-date (value "begin_date"))
			(map-slot 'project-members
				  (one-to-many 'project-member "project_id")
				  #'(lambda (&rest roles)
				      (reduce #'(lambda (table role)
						  (setf (gethash (user-of role) table) role)
						  table)
					      roles
					      :initial-value (make-hash-table :size (length roles))))
				  #'alexandria:hash-table-values)))

(map-class 'project-member :table-name "project_memebers" :primary-key '(project user)
	   :slots (list (map-slot 'project (many-to-one 'project "project_id"))
			(map-slot 'user (many-to-one 'user "user_id"))))

(map-class 'project-manager :table-name "project_managers" :superclasses '(project-member)
	   :slots (list))

;;(map-class 'project-plan :table-name "project_plans" :primary-key '(project id)
;;	   :slots (list (map-slot 'project (many-to-one 'project "project_id"))
;;			(map-slot 'id (column "id"))
;;			(map-slot 'root-entries (one-to-many 'root-entry ("project_id" "project_plan_id"))))

;;(map-class 'plan-entry :table-name "plan_entries" :primary-key '(project-plan id)
;;	   :slots (list (map-slot 'project-plan (many-to-one 'project-plan "project_id" "project_plan_id"))
;;			(map-slot 'id (simple-value "id"))
;;			(map-slot 'project-task (many-to-one 'project-task "project_id" "project_task_id"))
;;			(map-slot 'children (one-to-many 'child-entry "project_id" "project_plan_id" "parent_id"))))

;;(map-class 'root-entry :table-name "plan_root_entries" :superclasses '(plan-entry)
;;	   :slots (list))

;;(map-class 'child-entry :table-name "plan_child_entries" :superclasses '(plan-entry)
;;	   :slots (list (map-slot 'parent (many-to-one 'child-entry "project_id" "project_plan_id" "parent_id"))))

;;(map-class 'root-entry :table-name "plan_root_entries" :superclasses '(plan-entry)
;;	   :slots (list))

;; сделать отображение по первичных ключам более удобным

;; (map-slot 'project-plan (many-to-one 'project-plan "project_id"))
;;			(map-slot 'id (column "id"))))

;;(map-class 'project-task :table-name "project_tasks" :primary-key '(project id)
;;	   :slots (list (map-reference 'project 'project (map-value 'id "project_id"))
;;			(map-reference 'user 'user (map-value 'id "user_id"))))



;; map-slot как выходна автоматическоеотображение, когда указывается только название слота
;; и вся информация выводится из его содержимого

(defmacro define-mapping (name &body body)
  `(map-table name (quote ,@body)))

(defun map-table (name expression)
  (make-instance 'class-mapping :class (ma

;; вот как надо!!!!!!!!!!

(define-mapping "projects"
    #'(lambda (&key id name begin-date)
	(make-instance 'project :id id
		       :name name
		       :begin-date begin-date
		       :managers (reduce #'(lambda (hash-table manager)
					     (setf (gethash (user-of manager) hash-table) manager)
					     hash-table)
					 (db-read :all 'project-manager
						  :where #'(lambda (project-managmer)
							     (eq (id-of (project-of project-managment)) id)))))))

(define-mapping "projects"
	   #'(lambda (&key id name begin-date)
	       (make-instance 'project :id id
			      :name name
			      :begin-date begin-date
			      :managers (reduce #'(lambda (hash-table manager)
						    (setf (gethash (user-of manager) hash-table) manager)
						    hash-table)
						(db-read :all 'project-manager
							 :where #'(lambda (project-managmer)
								    (eq (id-of (project-of project-managment)) id)))))))
;; предположим, что slot mamanegers не имеет :initarg, а только initform
;; при загрузке обеъекта, все значения слотов, загрузка которых отложена, удаляются (slot-makunbound)

(map-table "projects"
	   #'(lambda (&key id name begin-date)
	       (let ((o (make-instance 'project :id id
			      :name name
			      :begin-date begin-date)))
		 (reduce #'(lambda (hash-table manager)
			     (setf (gethash (user-of manager) hash-table) manager)
			     hash-table)
			 (db-read :all 'project-manager
				  :where #'(lambda (project-managmer)
					     (eq (id-of (project-of project-managment)) id)))
			 :initial-value (managers-of o)))))

;; если у слота нет initarg, то у него должна быть initform. 

(map-table "users"
	   #'(lambda (&key id name login password)
	       (make-instance 'user :id id
			      :name name
			      :login login
			      :password password
			      :managed-projects (reduce #'(lambda (hash-table manager)
							    (setf (gethash (project-of manager) hash-table) manager)
							    hash-table)
							(db-read :all 'project-manager
								 :where #'(lambda (project-manager)
									    (eq (id-of (user-of project-manager)) id)))))))

(map-table "project_managers"
	   #'(lambda (&key project-id user-id)
	       (make-instance 'project-manager
			      :project (db-read :one 'project
						:where #'(lambda (project)
							   (eq (id-of project) project-id)))
			      :user (db-read :one 'user
					     :where #'(lambda (user)
							(eq (id-of user) user-if))))))

;; а теперь как это используется

(with-session ()
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

(defun schema ()
  (let* ((projects
	  (make-instance 'table :name "projects" :primary-key (list "id")
			 :columns (list
				   (make-instance 'column :name "id" :type "serial")
				   (make-instance 'column :name "name" :type "varchar")
				   (make-instance 'column :name "begin_date"
						  :type "timestump"))))
	 (users
	  (make-instance 'table :name "users" :primary-key (list "id")
			 :columns (list
				   (make-instance 'column :name "id" :type "serial")
				   (make-instance 'column :name "name" :type "varchar")
				   (make-instance 'column :name "email" :type "varchar")
				   (make-instance 'column :name "login" :type "varchar")
				   (make-instance 'column :name "password" :type "varchar"))))
	 (project-members
	  (make-instance 'table :name "project_members"
			 :primary-key (list "user_id" "project_id")
			 :columns (list
				   (make-instance 'column :name "user_id" :type "integer")
				   (make-instance 'column :name "project_id" :type "integer"))
			 :foreign-keys (list
					(make-instance 'foreign-key :table users
						       :columns (list
								 (cons "user_id" "id")))
					(make-instance 'foreign-key :table projects
						       :columns (list
								 (cons "project_id" "id"))))))
	 (project-managers
	  (make-instance 'table :name "project_managers"
			 :primary-key (list "user_id" "project_id")
			 :columns (list
				   (make-instance 'column :name "user_id" :type "integer")
				   (make-instance 'column :name "project_id" :type "integer"))
			 :foreign-keys (list
					(make-instance 'foreign-key :table users
						       :columns (list
								 (cons "user_id" "id")
								 (cons "project_id" "id")))))))
    (list projects users project-members project-managers)))

(defun schema ()
  (let* ((projects
	  (make-table "projects" (list "id")
		      (list (make-instance 'column :name "id" :type "serial")
			    (make-instance 'column :name "name" :type "varchar")
			    (make-instance 'column :name "begin_date" :type "timestump"))))
	 (users
	  (make-table "users" (list "id")
		      (list (make-instance 'column :name "id" :type "serial")
			    (make-instance 'column :name "name" :type "varchar")
			    (make-instance 'column :name "email" :type "varchar")
			    (make-instance 'column :name "login" :type "varchar")
			    (make-instance 'column :name "password" :type "varchar"))))
	 (project-members
	  (make-table "project_members" (list "user_id" "project_id")
		      (list
		       (make-instance 'column :name "user_id" :type "integer")
		       (make-instance 'column :name "project_id" :type "integer"))
		      (make-instance 'foreign-key :table users
				     :columns (list
					       (cons "user_id" "id")))
		      (make-instance 'foreign-key :table projects
				     :columns (list
					       (cons "project_id" "id")))))
	 (project-managers
	  (make-table "project_managers" (list "user_id" "project_id")
		      (list
		       (make-instance 'column :name "user_id" :type "integer")
		       (make-instance 'column :name "project_id" :type "integer"))
		      (list
		       (make-instance 'foreign-key :table users
				      :columns (list
						(cons "user_id" "id")
						(cons "project_id" "id"))))))
	 (list projects users project-members project-managers))))

(addtest (test-mappings) direct-mapping-definition
	 (schema))

(define-class-mapping user ("users" "id")
  (id (value "id"))
  (name (value "name"))
  (login (value "login"))
  (password (value "password"))
  (managed-projects (one-to-many 'project-manager 'user)
		    #'(lambda (&rest roles)
			(reduce #'(lambda (table role)
				    (setf (gethash (project-of role) table) role)
				    table)
				roles
				:initial-value (make-hash-table :size (length roles))))
		    #'alexandria:hash-table-values))

(map-class 'user :table-name "users" :primary-key '(id)
	   :slots (list #'(lambda (class-mapping)
			    (compute-slot-mapping class-mapping 'id
						  #'(lambda (mappings)
						      (value class-mapping "id"))))
			(map-slot 'name (value "name"))
			(map-slot 'login (value "login"))
			(map-slot 'password (value "password"))
			(map-slot 'managed-projects
				  (one-to-many 'project-manager 'user)
				  #'(lambda (&rest roles)
				      (reduce #'(lambda (table role)
						  (setf (gethash (project-of role) table) role)
						  table)
					      roles
					      :initial-value (make-hash-table :size (length roles))))
				  #'alexandria:hash-table-values)))

(map-class 'project "projects" '("id")
	   (map-slot 'id (value "id"))
	   (map-slot 'name (value 'name "name"))
	   (map-slot 'begin-date (value "begin_date"))
	   (map-slot 'project-members
		     (one-to-many 'project-member "project_id")
		     #'(lambda (&rest roles)
			 (reduce #'(lambda (table role)
				     (setf (gethash (user-of role) table) role)
				     table)
				 roles
				 :initial-value (make-hash-table :size (length roles))))
		     #'alexandria:hash-table-values)))

(map-class 'project-member :table-name "project_memebers" :primary-key '(project user)
	   :slots (list (map-slot 'project (many-to-one 'project "project_id"))
			(map-slot 'user (many-to-one 'user "user_id"))))

(map-class 'project-manager :table-name "project_managers" :superclasses '(project-member)
	   :slots (list))

(defclass persistence-unit ()
  ((direct-class-mappings :initarg :direct-class-mappings
		    :reader effective-mappings-of)
   (tables :initarg :tables
	   :reader tables-of)
   (effective-class-mappings :initarg :effective-class-mappings
		       :reader effective-mappings-of)))
	  
(defun make-persistence-unit (&rest direct-class-mappings)
  (make-instance 'persistence-unit
		 :direct-class-mappings direct-class-mappings))

(defmethod shared-initialize :after ((instance persistentce-unit) slot-names
				     &key direct-class-mappings)
  (with-slots (tables) instance ;;effective-class-mappings)
    (setf tables (reduce #'(lambda (alist direct-slot-mapping)
			     (let ((table-name (table-name-of direct-slot-mapping))
				   (columns (compute-columns table-name direct-class-mappings)))
			       (acons table-name
				      (make-instance 'table :name table-name :columns columns
						     :primary-key (map 'list
								       #'(lambda (column-name)
									   (assoc column-name columns))
								       (primary-key-of direct-slot-mapping)))
				      alist)))
			 direct-class-mappings))))

(defun compute-table-columns (direct-class-mapping direct-class-mappings)
  (append (compute-value-columns direct-class-mapping)
	  (compute-many-to-one-columns direct-class-mapping)
	  (compute
  (append (mapcar #'columns-of
		  (remove-if #'(lambda (mapping)
				 (typep mapping 'direct-one-to-many-mapping))
			     (mapcar #'direct-mapping-of
				     (direct-slot-mappings-of direct-class-mapping))))
	  (mapcar #'columns-of
		  (remove-if-not #'(lambda (mapping)
				     (typep mapping 'direct-one-to-many-mapping))
			     (mapcar #'direct-mapping-of
				     (direct-slot-mappings-of direct-class-mapping))))
		  
 ;;   :foregn-keys (compute-foreign-keys table-name columns direct-class-mappings))


(defun compute-table (direct-class-mapping direct-class-mappings)
  (make-instance 'table :name (table-name-of direct-class-mapping)
		 :columns (apply #'append
				 (mapcar #'columns-names-of
					 (direct-slot-mappings-of class-direct-mapping)))))
;;				  (apply #'append
;;					 (mapcar #'columns-names-of
;;						 (direct-class-mapping))
;;				  (apply #'append
;;					 (mapcar #'(lambda (direct-class-mapping)
;;						     (compute-foreign-key-columns-names-of direct-class-mappings)
						 
;;		 :columns (reduce #'(lambda (direct-class-mapping)
;;				      (
;;  (reduce #'(lambda (tables class-direct-mapping)
;;	      (let ((table-name (table-name-of class-direct-mapping))
;;		    (foreign-keys (compute-foreign-keys (class-name-of class-direct-mapping)
;;							class-direct-mappings)))
;;		(setf (gethash table-name tables)
;;		      (make-instance 'table :name table-name
;;				     :foregn-keys foreign-keys
;;				     

;;(reduce-to-hash-table #'table-name-of
;;		      (map 'list
;;			   #'(lambda (direct-class-mapping)
;;			       (compute-table ))
;;			   direct-class-mappings)))

;;   (class-name :initarg :class-name :reader class-name-of)
;;   (superclasses-names :initarg :superclasses :reader superclasses-of)
;;   (value-mappings :initarg :value-mappings :reader value-mappings-of)
;;   (reference-mappings :initarg :reference-mappings :reader reference-mappings-of)
;;   (collection-mappings :initarg :collection-mappings :reader collection-mappings-of)
;;   ))

;;	      effective-class-mappings (compute-effective-class-mappings direct-class-mappings
;;									 computed-tables)))))

;;(defun reduce-to-hash-table (key sequence)
;;  (reduce #'(lambda (hash-table object)
;;	      (setf (gethash (funcall key object) hash-table) object)
;;	      hash-table)
;;	  sequence
;;	  :initial-value (make-hash-table :size (length sequence))))

;;(defun compute-effective-class-mappings (direct-class-mappings tables)
;;  (let ((effective-class-mappings (reduce-to-hash-table #'class-name-of
;;							(map 'list
;;							     #'(lambda (direct-class-mapping)
;;								 (compute-effective-class-mapping direct-class-mapping
;;												  (gethash (table-name-of direct-class-mapping) tables)))
;;							     direct-class-mappings))))
;;    (maphash #'(lambda (class-name effective-class-mapping)
;;		 (declare (ignore class-name))
;;		 (with-slots (superclasses-mappings many-to-one-mappings one-to-many-mappings) effective-class-mapping
;;		   (setf superclasses-mappings
;;			 (compute-superclasses-mappings effective-class-mapping
;;							effective-class-mappings)
;;			 many-to-one-mappings
;;			 (compute-many-to-one-mappings effective-class-mapping
;;						       effective-class-mappings)
;;			 one-to-many-mappings
;;			 (compute-one-to-many-mappings effective-class-mapping
;;						       effective-class-mappings))))
;;	     effective-class-mappings)))

;;(defun make-effective-class-mapping (					 (make-instance 'effective-class-mapping
;;							:class-name (class-name-of direct-class-mapping)
;;							:table (gethash (table-name-of direct-class-mapping) tables)))))))



;;(defun compute-effective-class-mapping (direct-class-mapping table direct-class-mappings)
;;  (reduce #'(lambda (tables class-direct-mapping)
;;	      (let ((table-name (table-name-of class-direct-mapping))
;;		    (foreign-keys (compute-foreign-keys (class-name-of class-direct-mapping)
;;							class-direct-mappings)))
;;		(setf (gethash table-name tables)
;;		      (make-instance 'table :name table-name
;;				     :foregn-keys foreign-keys
;;				     :columns (append (reduce #'append
;;							      (mapcar #'columns-of
;;								      (value-mappings-of class-direct-mapping)))
;;						      (reduce #'append
;;							      (mapcar #'columns-of foreign-keys)))))))))
;;

;; загрузка начинается с отображения не имеющего ассоциаций many-to-one



;;доинициализировать коллекции при создании зависимых объектов

;;(defclass effective-slot-mapping ()
;;  ((slot-name :initarg :slot-name :reader slot-name-of)
;;   (effective-mapping :initarg :mapping :reader effective-mapping-of)
;;   (constructor :initarg :constructor :reader constructor-of)
;;   (reader :initarg :reader :reader reader-of)))

;;(defclass effective-value-mapping ()
;;  ((columns :initarg :columns :reader columns-of)))

;;(defclass effective-one-to-many-mapping ()
;;  ((class-name :initarg :class-name :reader class-name-of)
;;   (columns :initarg :columns :reader columns-of)))

;;(defclass effective-many-to-one-mapping ()
;;  ((class-name :initarg :class-name :reader class-name-of)
;;   (columns :initarg :columns :reader columns-of)))

;;(defun make-persistence-unit (&rest class-mappings)
;;  (reduce #'compute-mapping class-mappings
;;	  :initial-value (make-instance 'persistence-unit)))

;(defun compute-mapping (persistence-unit class-mapping)
;  (make-instance 'persistence-unit
;		 
	;	 :tables (compute-tables (tables-of persistence-unit))
	;	 :class-mappings
		 ;;(lambda (persistence-unit class-mapping)
	      ;(make-instance 'persistence-unit :tables



;;compute-columns table-name mappings)))))))

;;(defun compute-columns (table-name class-mappings)
;;  (reduce #'(lambda (columns slot-mapping-definition)
	      

;;(defun column (name) ;;? может для определения типа столбца
;;  (make-instance 'column :name name))





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

(defun make-select-query (class-mapping)
  (let* ((table-reference (make-instance 'table-reference
					 :table (table-of class-mapping)))
	 (table-expression (make-instance 'table-expression
					  :table-reference table-reference
					  :joins (compute-joins table-reference
								class-mapping))))
    (make-instance 'select-query
		   :select-list-items 
		   :table-expression table-expression)))

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


(defmethod initialize-instance :after ((instance class-mapping) &key
				       mapping-definition
				       mapping-schema)
  (with-slots (configuration table superclasses-mappings
			     value-mappings reference-mappings) instance
    (setf table (get-table (table-name-of mapping-definition) mapping-schema)
	  superclasses-mappings (mapcar #'(lambda (class)
					    (get-mapping class mapping-schema))
					(superclasses-of mapping-definition))
	  value-mappings (mapcar #'(lambda (slot-mapping-definition))
				 (value-mappingf-of definition))
	  reference-mappings (mapcar #'(lambda (slot-mapping-definition))
				       (reference-mappings-of definition))


value-mappings (mapcar #'(lambda (slot-mapping-definition))
				   (value-mappingf-of definition))
	    reference-mappings (mapcar #'(lambda (slot-mapping-definition))
				       (reference-mappings-of definition))
	    superclasses-mappings (mapcar #'(lambda (mapped-class)
					      (extend (get-mapping mapped-class configuration)
						      instance))
					  (superclasses-of definition))))))
	  
				     
  	(reduce #'(lambda (hash-table definition)
		    (let ((mapped-class (mapped-class-of definition)))
		      (setf (gethash mapped-class hash-table)
					    (make-instance 'class-mapping
							   :mapped-class mapped-class
							   :table )))
				    hash-table)
				class-mapping-definitions
				:initial-value )))