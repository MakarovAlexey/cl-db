(in-package #:cl-db)

(defclass user ()
  ((id :initarg :id
       :reader id-of
       :mapping-type :property
       :columns (("id" "uuid")))
   (name :initarg :name
	 :accessor name-of
	 :mapping-type :property
	 :columns (("name" "varchar")))
   (login :initarg :login
	  :accessor login-of
	  :mapping-type :property
	  :columns (("login" "varchar")))
   (password :initarg :password
	     :accessor password-of
	     :mapping-type :property
	     :columns (("password" "varchar")))
   (project-managments :reader project-managments-of
		       :mapping-type :one-to-many
		       :referenced-class project-managment
		       :index-by #'project-of
		       :columns ("user_id"))
   (project-participations :reader project-participations
			   :mapping-type :one-to-many
			   :referenced-class project-participation
			   :index-by #'project-of
			   :columns ("user_id")))
  (:metaclass persistent-class)
  (:table-name "users")
  (:primary-key "id"))

(defclass project-participation ()
  ((project :initarg :project
	    :reader project-of
	    :mapping-type :many-to-one
	    :referenced-class project
	    :columns ("project_id"))
   (user :initarg :user
	 :reader user-of
	 :mapping-type :many-to-one
	 :referenced-class user
	 :columns ("user_id")))
  (:metaclass persistent-class)
  (:table-name "project_memebers")
  (:primary-key ("project_id" "user_id")))

(defclass project-managment (project-participation)
  ()
  (:metaclass persistent-class)
  (:table-name "project_managers")
  (:primary-key "project_id" "user_id"))

(defclass project ()
  ((id :initarg :id
       :reader id-of
       :mapping-type :property
       :columns (("id" "uuid")))
   (name :initarg :name
	 :accessor name-of
	 :mapping-type :property
	 :columns (("name" "varchar")))
   (begin-date :initarg :begin-date
	       :accessor begin-date-of
	       :mapping-type :property
	       :columns (("begin_date" "date")))
   (project-members :accessor :project-members
		    :mapping-type :one-to-many
		    :referenced-class project-member
		    :index-by #'user-of
		    :columns(("project_id"))))
   (:metaclass persistent-class)
   (:table-name "projects")
   (:primary-key ("id")))

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
