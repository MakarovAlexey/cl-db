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
   (project-manager-roles :initarg :project-manager-roles
			  :accessor project-manager-roles-of))
  (:documentation "Пользователь системы, ответственный исполнитель"))

(defclass project ()
  ((name :initarg :name
	 :accessor name-of)
   (begin-date :initarg :begin-date
	       :accessor begin-date-of)
   (project-members :initarg :project-members
		    :accessor project-members-of)))

(defclass project-member ()
  ((project :initarg :project :reader project-of)
   (user :initarg :user :reader user-of)))

(defclass project-manager (project-member)
  ())

(defun map-user ()
  (map-class 'user "users" '(id)
	     :slots (list (map-slot 'id (value "id"))
			  (map-slot 'name (value "name"))
			  (map-slot 'login (value "login"))
			  (map-slot 'password (value "password"))
			  (map-slot 'project-manager-roles (one-to-many 'project-manager "user_id")
				    #'(lambda (&rest roles)
					(reduce #'(lambda (table role)
						    (setf (gethash (project-of role) table) role)
						    table)
						roles
						:initial-value (make-hash-table :size (length roles))))
				    #'alexandria:hash-table-values))))

(defun map-project ()
  (map-class 'project "projects" '("id")
	     :slots (list (map-slot 'id (value "id"))
			  (map-slot 'name (value "name"))
			  (map-slot 'begin-date (value "begin_date"))
			  (map-slot 'project-members
				    (one-to-many 'project-member "project_id")
				    #'(lambda (&rest roles)
					(reduce #'(lambda (table role)
						    (setf (gethash (user-of role) table) role)
						    table)
						roles
						:initial-value (make-hash-table :size (length roles))))
				    #'alexandria:hash-table-values))))

(defun map-project-member ()
  (map-class 'project-member "project_memebers" '("project_id" "user_id")
	     :slots (list (map-slot 'project (many-to-one 'project "project_id"))
			  (map-slot 'user (many-to-one 'user "user_id")))))

(defun map-project-manager ()
  (map-class 'project-manager "project_managers" '("project_id" "user_id")
	     :superclasses '(project-member)))

(lift:deftestsuite schema-test ()
  ((schema (make-instance 'mapping-schema
			  :class-mapping-definitions (list
						      (map-project)
						      (map-user)
						      (map-project-member)
						      (map-project-manager))))))

(defun contains-only (sequence &rest elements)
  (let ((excess-elements (set-difference sequence elements :test #'equalp))
	(expected-elements (set-difference elements sequence :test #'equalp)))
    (lift:ensure-null excess-elements
		      :report "Sequence ~a have excess elements ~a"
		      :arguments (excess-elements sequence))
    (lift:ensure-null expected-elements
		      :report "In sequence ~a expected ~a"
		      :arguments (excess-elements sequence))))

(lift:addtest table-columns
  (contains-only (mapcar #'name-of
			 (alexandria:hash-table-values
			  (columns-of (get-table "projects" schema))))
		 (list "id" "name" "begin_date"))
  (contains-only (mapcar #'name-of
			 (alexandria:hash-table-values
			  (columns-of (get-table "users" schema))))
		 (list "id" "name" "login" "password"))
  (contains-only (mapcar #'name-of
			 (alexandria:hash-table-values
			  (columns-of (get-table "project_memebers" schema))))
		 (list "user_id" "project_id"))
  (contains-only (mapcar #'name-of
			 (alexandria:hash-table-values
			  (columns-of (get-table "project_managers" schema))))
		 (list "project_id" "user_id")))

(lift:addtest subclasses
  (lift:ensure-null
    (not (subclasses-mappings-of
	  (get-mapping (find-class 'project-member) schema)))))

(lift:addtest value-mappings
  (lift:ensure-null
    (not (value-mappings-of
	  (get-mapping (find-class 'project) schema)))))

(lift:addtest reference-mappings
  (lift:ensure (every #'(lambda (reference)
			  (not (null reference)))
		      (reference-mappings-of
		       (get-mapping (find-class 'project) schema)))))

(lift:addtest object-loaders
  (lift:ensure-failed-error
   (make-object-loader (get-mapping (find-class 'project) schema) nil)
   (make-object-loader (get-mapping (find-class 'user) schema) nil)
   (make-object-loader (get-mapping (find-class 'project-member) schema) nil)
   (make-object-loader (get-mapping (find-class 'project-manager) schema) nil)))