(in-package #:cl-db)

(defclass user ()
  ((id :initarg :id
       :reader id-of
       :property t
       :colmns (("id" "uuid")))
   (name :initarg :name
	 :accessor name-of
	 :property t
	 :columns (("name" "varchar")))
   (login :initarg :login
	  :accessor login-of
	  :property t
	  :columns (("login" "varchar")))
   (password :initarg :password
	     :accesor password-of
	     :property t
	     :columns (("password" "varchar")))
   (project-managments :reader project-managments-of
		       :one-to-many project-managment
		       :index-by #'project-of
		       :columns ("user_id"))
   (project-participations :reader project-participations
			   :one-to-many project-managment
			   :index-by #'project-of
			   :columns ("user_id")))
  (:metaclass persistent-class)
  (:table "users")
  (:primary-key "id"))

(defclass project-participation ()
  ((project :initarg :project
	    :reader project-of
	    :many-to-one project
	    :columns ("project_id"))
   (user :initarg :user
	 :reader user-of
	 :many-to-one user
	 :columns ("user_id"))
  (:metaclass persistent-class)
  (:table "project_memebers")
  (:primary-key ("project_id" "user_id")))

(defclass project-managment (project-participation)
  ()
  (:metaclass persistent-class)
  (:table "project_managers")
  (:primaty-key "project_id" "user_id"))

(defclass project ()
  ((id :initarg :id
       :reader id-of
       :property t
       :columns (("id" "uuid")))
   (name :initarg :name
	 :accessor name-of
	 :property t
	 :columns (("name" "varchar")))
   (begin-date :initarg :begin-date
	       :accessor begin-date-of
	       :property t
	       :columns (("begin_date" "date")))
   (project-members :accessor :project-members
		    :one-to-many project-member
		    :index-by #'user-of
		    :columns(("project_id"))))
   (:metaclass persistent-class)
   (:table "projects")
   (:primay-key ("id")))

(define-database-interface postgresql-postmodern
  (:open-connection #'cl-postgres:open-database)
  (:close-connection #'cl-postgres:close-database)
  (:prepare #'cl-postgres:prepare-query)
  (:execute
   #'(lambda (connection name &rest params)
       (cl-postgres:exec-prepared connection name params
				  'cl-postgres:alist-row-reader))))

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
