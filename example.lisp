(in-package #:ucl-glorp)

(defclass persistence-unit ()
  ((class-maps :initform (make-hash-table) :reader class-maps-of)))

(defclass class-map ()
  ((mapped-class :initarg :class :reader mapped-class-of)
   (table-name :initarg :table-name :reader table-name-of)
   (slot-mappings :initarg :slot-mappings :reader slot-mappings-of)
   (primary-key :initarg :prinary-key :reader primary-key-of)))

(defun map-class (class-name &key table-name primary-key slots discriminator-value)
  (make-instance 'class-map :class (find-class 'class-name)
		 :table-name table-name
		 :slot-maps slots
		 :primary-key primary-key))

(defclass slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)))

(defclass value-mapping (slot-mapping)
  ((column :initarg :column :reader column-of)))

(defun value (slot-name column-name)
  (make-instance 'value-mapping :column column-name))

;;(defun one-to-many (slot-name 

(defun map-hash-table (key-mapping value-mapping)
  (make-instance 'hash-table-mapping
		 :key-mapping key-mapping
		 :value-mapping value-mapping))

(defclass reference-mapping (slot-mapping)
  ((referenced-class :initarg :referenced-class :reader referenced-class-of)
   (columns :initarg :columns :reader columns-of)))

(defun map-reference (class-name &rest columns)
  (make-instance 'reference-mapping
		 :referenced-class (find-class class-name)
		 :columns columns))

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
(defun map-user ()
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
				    #'alexandria:hash-table-values))))

(map-class 'project :table-name "projects" :primary-key '(id)
	   :slots (list (map-slot 'id (value "id"))
			(map-slot 'name (value 'name "name"))
			(map-slot 'begin-date (value "begin_date"))
			(map-slot 'project-members
				  (one-to-many 'project-member 'project)
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

(list (where (query-over 'project)
       #'(lambda (project)
	   (eq project 1))))
		       
		   (
	    :where (and-expression )

;; сделать db-read макросом? или оставить как функцию, а переделывать выражение в with-session?
;; осталось написать

(make-instance 'registry
	       :mappers (list (make-instance 'mapper :class (find-class 'project)
					     :select-all '(:select id name :from projects)
					     :select