(defpackage #:cl-db.tests
  (:use #:cl #:cl-db #:lift))

(in-package #:cl-db.tests)

(deftestsuite test-mappings () ())

(defclass user ()
  ((name :initarg :name :accessor name-of)
   (login :initarg :login :accessor login-of)
   (password :initarg :password :accessor password-of)
   (email :initarg :email :accessor email-of))
  (:documentation "Пользователь системы, ответственный исполнитель"))

(defun map-user ()
  (map-class 'user "users" '(id)
	     (map-slot 'id (value "id"))
	     (map-slot 'name (value "name"))
	     (map-slot 'login (value "login"))
	     (map-slot 'password (value "password"))
	     (map-slot 'managed-project (one-to-many 'project-manager "user_id")
		       #'(lambda (&rest roles)
			   (reduce #'(lambda (table role)
				       (setf (gethash (project-of role) table) role)
				       table)
				   roles
				   :initial-value (make-hash-table :size (length roles))))
		       #'alexandria:hash-table-values)))

(defclass project ()
  ((name :initarg :name :accessor name-of)
   (begin-date :initarg :begin-date :accessor begin-date-of)))

(defun map-project ()
  (map-class 'project "projects" '(id)
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

(defclass project-member ()
  ((project :initarg :project :reader project-of)
   (user :initarg :user :reader user-of)))

(defun map-project-member ()
  (map-class 'project-member "project_memebers" '("project_id" "user_id")
	     (map-slot 'project (many-to-one 'project "project_id"))
	     (map-slot 'user (many-to-one 'user "user_id"))))

(defclass project-manager (project-member)
  ())

(defun map-project-manager ()
  (map-class 'project-manager "project_managers" '("project_id" "user_id")))

(addtest (test-mappings) direct-mapping-definition
  (progn (map-project)
	 (map-user)
	 (map-project-member)))
			 ;;(map-project-manager)))