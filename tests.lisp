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
  (map-class 'project "projects" '(id)
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
  (map-class 'project-manager "project_managers" '("project_id" "user_id")))

(lift:deftestsuite test-mappings ()
  ((session
    (make-clos-session #'(lambda () nil)
		       (map-project)
		       (map-user)
		       (map-project-member)
		       (map-project-manager)))))

(lift:addtest (test-mappings) classes-mapped
	      (assert (get-class-mapping session (find-class 'project)))
	      (assert (get-class-mapping session (find-class 'user)))
	      (assert (get-class-mapping session (find-class 'project-member)))
	      (assert (get-class-mapping session (find-class 'project-manager))))

(lift:addtest (test-mappings) object-loaders
	      (with-session (session)
		(make-instance 'object-loader
			       :class-mapping (get-class-mapping session
								 (find-class 'project)))))

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

;;(lift:addtest (test-mappings) reading-all-projects
;;	      (with-session (session)
;;		(db-read :all 'project)))

;;(lift:addtest (test-mappings) executing-query
;;	      (make-query 'project #'project-manager-roles-of))

;;(lift:addtest (test-mappings) inheritnce-query
;;	      (make-query ))

;;(lift:addtest (test-mappings) inheritnce-query-with-fetch
;;	      (make-query 'project-managment #'user-of))
	 
			 ;;(map-project-manager)))