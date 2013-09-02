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

(defclass project-role ()
  ((project :initarg :project :reader project-of)
   (user :initarg :user :reader user-of)))

(defclass project-participation (project-role)
  ())

(defclass project-managment (project-role)
  ())

(define-mapping-schema test-mapping)

(define-class-mapping (user "users")
    ((:primary-key "id"))
  (id (:value ("id" "integer")))
  (name (:value ("name" "varchar")))
  (login (:value ("login" "varchar")))
  (password (:value ("password" "varchar")))
  (project-participations
   (:one-to-many project-participation "user_id")
			 #'(lambda (&rest roles)
			     (reduce #'(lambda (table role)
					 (setf (gethash (project-of role) table) role)
					 table)
				     roles
				     :initial-value (make-hash-table :size (length roles))))
			 #'alexandria:hash-table-values)
  (project-managments
   (:one-to-many project-managment "user_id")
   #'(lambda (&rest roles)
       (reduce #'(lambda (table role)
		   (setf (gethash (project-of role) table) role)
		   table)
	       roles
	       :initial-value (make-hash-table :size (length roles))))
   #'alexandria:hash-table-values))

(define-class-mapping (project "projects")
    ((:primary-key "id"))
  (id (:value ("id" "integer")))
  (name (:value ("name" "varchar")))
  (begin-date (:value ("begin_date" "timestamp")))
  (project-roles
   (:one-to-many project-role "project_id")
   #'(lambda (&rest roles)
       (reduce #'(lambda (table role)
		   (setf (gethash (user-of role) table) role)
		   table)
	       roles
	       :initial-value (make-hash-table :size (length roles))))
   #'alexandria:hash-table-values))

(define-class-mapping (project-role "project_roles")
    ((:primary-key "project_id" "user_id"))
  (project (:many-to-one project "project_id"))
  (user (:many-to-one user "user_id")))
  
(define-class-mapping (project-participation "project_participations")
    ((:superclasses project-role)))

(define-class-mapping (project-managment "project_managments")
    ((:superclasses project-role)))

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

