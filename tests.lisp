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

(defclass project ()
  ((name :initarg :name
	 :accessor name-of)
   (begin-date :initarg :begin-date
	       :accessor begin-date-of)
   (project-members :initarg :project-members
		    :accessor project-members-of)))

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

(defclass project-member ()
  ((project :initarg :project :reader project-of)
   (user :initarg :user :reader user-of)))

(defun map-project-member ()
  (map-class 'project-member "project_memebers" '("project_id" "user_id")
	     :slots (list (map-slot 'project (many-to-one 'project "project_id"))
			  (map-slot 'user (many-to-one 'user "user_id")))))

(defclass project-manager (project-member)
  ())

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

(lift:addtest table-columns
	      (lift:ensure-same
	       (reverse
		(mapcar #'name-of
			(alexandria:hash-table-values
			 (columns-of (get-table "projects" schema)))))
	       (list "id" "name" "begin_date"))
	      (lift:ensure-same
	       (reverse
		(mapcar #'name-of
			(alexandria:hash-table-values
			 (columns-of (get-table "users" schema)))))
	       (list "id" "name" "login" "password"))
	      (lift:ensure-same
	       (reverse
		(mapcar #'name-of
			(alexandria:hash-table-values
			 (columns-of (get-table "project_memebers" schema)))))
	       (list "user_id" "project_id"))
	      (lift:ensure-same
	       (reverse
		(mapcar #'name-of
			(alexandria:hash-table-values
			 (columns-of (get-table "project_managers" schema)))))
	       (list "project_id" "user_id")))


