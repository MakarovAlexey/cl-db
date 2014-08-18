(in-package #:cl-db)

;;(define-database-interface postgresql-postmodern
;;  (:open-connection #'cl-postgres:open-database)
;;  (:close-connection #'cl-postgres:close-database)
;;  (:prepare #'cl-postgres:prepare-query)
;;  (:execute
;;   #'(lambda (connection name &rest params)
;;       (cl-postgres:exec-prepared connection name params
;;				  'cl-postgres:alist-row-reader))))

(lift:deftestsuite session ()
  ())

;;(lift:addtest session-open-and-close
;;  (lift:ensure-no-warning
;;    (with-session ((:mapping-schema cats-mapping)
;;		   (:database-interface postgresql-postmodern)
;;		   (:connection-args "projects" "makarov"
;;				     "zxcvb" "localhost")))))

;;(lift:deftestsuite query ()
;;  ()
;;  (:dynamic-variables
;;   (*mapping-schema* (make-mapping-schema 'projects-managment))))

(define-schema projects-managment ()
  (user
   (("users" "id"))
   (id (:property ("id" "uuid")))
   (name (:property ("name" "varchar")))
   (login (:property ("login" "varchar")))
   (password (:property ("password" "varchar")))
   (project-managments
    (:one-to-many project-managment "user_id")
    #'(lambda (&rest roles)
	(alexandria:alist-hash-table
	 (mapcar #'(lambda (role)
		     (cons (project-of role) role))
		 roles)))
    #'alexandria:hash-table-values)
   (project-participations
    (:one-to-many project-managment "user_id")
    #'(lambda (&rest roles)
	(alexandria:alist-hash-table
	 (mapcar #'(lambda (role)
		     (cons (project-of role) role))
		 roles)))
    #'alexandria:hash-table-values))
  (project-participation
   (("project_memebers" "project_id" "user_id"))
   (project (:many-to-one project "project_id"))
   (user (:many-to-one user "user_id")))
  (project-managment
   (("project_managers" "project_id" "user_id")
    (project-participation "project_id" "user_id")))
  (project
   (("projects" "id"))
   (id (:property ("id" "uuid")))
   (name (:property ("name" "varchar")))
   (begin-date (:property ("begin_date" "date")))
   (objects
    (:many-to-one project-root-object "project_id"))
   (document-directories
    (:many-to-one root-document-directory "project_id"))
   (document-registrations
    (:one-to-many document-registration "project_id")
    #'(lambda (&rest registrations)
	(alexandria:alist-hash-table
	 (mapcar #'(lambda (registration)
		     (cons (document-of registration)
			   registration))
		 registrations)))
    #'alexandria:hash-table-values)
   (project-members
    (:one-to-many project-member "project_id")
    #'(lambda (&rest roles)
	(alexandria:alist-hash-table
	 (mapcar #'(lambda (role)
		     (cons (user-of role) role))
		 roles)))
    #'alexandria:hash-table-values)))
