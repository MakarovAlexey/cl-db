(in-package #:cl-db)

;;(define-database-interface postgresql-postmodern
;;  (:open-connection #'cl-postgres:open-database)
;;  (:close-connection #'cl-postgres:close-database)
;;  (:prepare #'cl-postgres:prepare-query)
;;  (:execute
;;   #'(lambda (connection name &rest params)
;;       (cl-postgres:exec-prepared connection name params
;;				  'cl-postgres:alist-row-reader))))

;;(lift:deftestsuite session ()
;;  ())

;;(lift:addtest session-open-and-close
;;  (lift:ensure-no-warning
;;    (with-session ((:mapping-schema cats-mapping)
;;		   (:database-interface postgresql-postmodern)
;;		   (:connection-args "projects" "makarov"
;;				     "zxcvb" "localhost")))))

(defclass user ()
  ((id :initarg :id
       :reader id-of)
   (name :initarg :name
	 :accessor name-of)
   (login :initarg :name
	  :accessor login-of)
   (password :initarg :password
	     :accessor password-of)
   (project-managments :initarg :project-managments
		       :accessor project-managments-of)
   (project-participations :initarg :project-participations
			   :accessor project-participations-of)))

(defclass project-participation ()
  ((project :initarg :project
	    :reader project-of)
   (user :initarg :user
	 :reader user-of)))

(defclass project-managment (project-participation)
  ())

(defclass project ()
  ((id :initarg :id
       :reader id-of)
   (name :initarg :name
	 :accessor name-of)
   (begin-date :initarg :begin-date
	       :accessor begin-date-of)
   (project-members :initarg :project-members
		    :accessor project-members-of)))

(define-schema projects-managment ()
  (user
   (("users" "id"))
   (id (:property "id" "uuid"))
   (name (:property "name" "varchar"))
   (login (:property "login" "varchar"))
   (password (:property "password" "varchar"))
   (project-managments
    (:one-to-many project-managment "user_id")
    #'(lambda (&rest roles)
	(alexandria:alist-hash-table
	 (mapcar #'(lambda (role)
		     (cons (project-of role) role))
		 roles)))
    #'alexandria:hash-table-values)
   (project-participations
    (:one-to-many project-participation "user_id")
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
   (id (:property "id" "uuid"))
   (name (:property "name" "varchar"))
   (begin-date (:property "begin_date" "date"))
   (project-members
    (:one-to-many project-participation "project_id")
    #'(lambda (&rest roles)
	(alexandria:alist-hash-table
	 (mapcar #'(lambda (role)
		     (cons (user-of role) role))
		 roles)))
    #'alexandria:hash-table-values)))

(lift:deftestsuite query-execute-tests ()
  ()
  (:dynamic-variables
   (*mapping-schema* (projects-managment))))

(lift:addtest list-projects
  (lift:ensure
   (db-read 'project)))

(lift:addtest slot-definition-found
  (lift:ensure
   (get-slot-definition (find-class 'project) #'id-of)))

(lift:addtest slot-definition-not-found
  (lift:ensure-error
    (get-slot-definition (find-class 'project) #'project-of)))

(lift:addtest property-found
  (multiple-value-bind (select-list references fetch)
      (db-read 'project
	       :select-list #'(lambda (project)
				(property project #'name-of))
	       :mapping-schema (projects-managment))
    (lift:ensure
     (first select-list))))

(lift:addtest select-properties-list
  (multiple-value-bind (select-list references fetch)
      (db-read 'project
	       :select-list #'(lambda (project)
				(property project #'name-of))
	       :mapping-schema (projects-managment))
    (lift:ensure
     (listp select-list))))

(lift:addtest select-single-property
  (multiple-value-bind (select-list references fetch)
      (db-read 'project
	       :select-list #'(lambda (project)
				(property project #'name-of))
	       :mapping-schema (projects-managment))
    (lift:ensure
     (not (listp select-list)))))

(lift:addtest list-objects
  (multiple-value-bind (select-list references fetch)
      (db-read 'project :mapping-schema (projects-managment))
    (lift:ensure
     (not (listp select-list)))))

(lift:addtest list-objects
  (multiple-value-bind (select-list references fetch)
      (db-read 'user :mapping-schema (projects-managment)
	       :where #'(lambda (user)
			  (db-eq (propery user #'login-of) "user")))))

(lift:addtest check-schema
  (destructuring-bind (class-name &key properties &allow-other-keys)
      (fourth *mapping-schema*)
    (declare (ignore class-name))
    (lift:ensure properties)))

(lift:addtest make-join-plan
  (let ((*table-index* 0))
    (make-join-plan (projects-managment) 'project)
    (make-join-plan (projects-managment) 'project-participation)
    (make-join-plan (projects-managment) 'project-managment)
    (make-join-plan (projects-managment) 'user)))

(lift:addtest join-reference
  (multiple-value-bind (select-list references fetch)
      (db-read 'project :mapping-schema (projects-managment)
	       :join #'(lambda (project)
			 (join project #'project-members-of :members)))
    (lift:ensure
     (not (listp select-list)))))

(lift:addtest fetch-reference
  (db-read 'project :mapping-schema (projects-managment)
	   :fetch #'(lambda (project)
		      (fetch project #'project-members-of
			     #'(lambda (member)
				 (fetch member #'user-of))))))

(lift:addtest ascending-order ()
  (db-read 'project
	   :mapping-schema (projects-managment)
	   :order-by #'(lambda (project)
			 (ascending project #'name-of))))

(lift:addtest joining ()
  (db-read 'project
	   :mapping-schema (projects-managment)
	   :join #'(lambda (project)
		     (join project #'project-members-of :members))
	   :select #'(lambda (project &key members)
		       (values project members))))

(lift:addtest get-property ()
  (db-read 'project
	   :mapping-schema (projects-managment)
	   :join #'(lambda (project)
		     (join project #'project-members-of :members))
	   :select #'(lambda (project &key members)
		       (values (property project #'name-of) members))))

(lift:addtest fetch-references
  (let* ((*table-index* 0)
	 (*mapping-schema* (projects-managment))
	 (join-plan (make-join-plan *mapping-schema* 'project)))
    (multiple-value-bind (select-list fetch-references)
	(apply #'compute-select join-plan)
      (multiple-value-bind (query loaders)
	  (apply #'compute-select-clause select-list)
	(let* ((fetch-expressions
		(reduce #'(lambda (result fetch-expression)
			    (multiple-value-call #'acons
			      (funcall fetch-expression) result))
			(multiple-value-list
			 (apply #'(lambda (project)
				    (fetch project #'project-members-of))
				fetch-references))
			:initial-value nil))
	       (fetch-expressions-by-loaders
		(reduce #'(lambda (result loader)
			    (list* loader
				   (mapcar #'first
					   (remove loader
						   fetch-expressions
						   :test-not #'eq
						   :key #'rest))
				   result))
			loaders :initial-value nil)))
	  (mapcar #'(lambda (loader)
		      (lift:ensure
		       (not
			(null
			 (getf fetch-expressions-by-loaders
			       loader)))))
		  loaders))))))

(lift:addtest fetch-references
  (let* ((*table-index* 0)
	 (*mapping-schema* (projects-managment))
	 (join-plan (make-join-plan *mapping-schema* 'project)))
    (multiple-value-bind (select-list fetch-references)
	(apply #'compute-select join-plan)
      (multiple-value-bind (query loaders)
	  (apply #'compute-select-clause select-list)
	(let* ((fetch-expressions
		(reduce #'(lambda (result fetch-expression)
			    (multiple-value-call #'acons
			      (funcall fetch-expression) result))
			(multiple-value-list
			 (apply #'(lambda (project)
				    (fetch project #'project-members-of))
				fetch-references))
			:initial-value nil))
	       (fetch-expressions-by-loaders
		(reduce #'(lambda (result loader)
			    (list* loader
				   (mapcar #'first
					   (remove loader
						   fetch-expressions
						   :test-not #'eq
						   :key #'rest))
				   result))
			loaders :initial-value nil)))
	  (mapcar #'(lambda (loader)
		      (lift:ensure
		       (not
			(null
			 (getf fetch-expressions-by-loaders
			       loader)))))
		  loaders))))))

(lift:addtest append-fetch-expressions-empty-references
  (let* ((*table-index* 0)
	 (*mapping-schema* (projects-managment))
	 (join-plan (make-join-plan *mapping-schema* 'project 'user)))
    (multiple-value-bind (select-list fetch-references)
	(apply #'compute-select
	       (multiple-value-list
		(apply #'(lambda (project user)
			   (values project
				   (db-count user)
				   (property user #'name-of)
				   (property user #'name-of)))
		       join-plan)))
      (multiple-value-bind (query loaders)
	  (apply #'compute-select-clause select-list)
	(let* ((fetch-expressions
		(reduce #'(lambda (result fetch-expression)
			    (multiple-value-call #'acons
			      (funcall fetch-expression) result))
			(multiple-value-list
			 (apply #'(lambda (project &rest args)
				    (declare (ignore args))
				    (fetch project #'project-members-of))
				fetch-references))
			:initial-value nil))
	       (fetch-expressions-by-loaders
		(reduce #'(lambda (result loader)
			    (list* loader
				   (mapcar #'first
					   (remove loader
						   fetch-expressions
						   :test-not #'eq
						   :key #'rest))
				   result))
			loaders :initial-value nil)))
	  (lift:ensure
	   (not (eq (apply #'append-fetch-expressions query
			   fetch-expressions-by-loaders)
		    query))))))))

(lift:addtest expression-group-by-clause
  (let* ((*table-index* 0)
	 (*mapping-schema* (projects-managment))
	 (join-plan (make-join-plan *mapping-schema* 'project 'user)))
    (destructuring-bind (project-expression user-count-expression)
	(apply #'compute-select
	       (multiple-value-list
		(apply #'(lambda (project user)
			   (values project
				   (db-count user)))
		       join-plan)))
      (lift:ensure
       (not
	(null (group-by-clause-of project-expression))))
      (lift:ensure
       (null
	(group-by-clause-of user-count-expression))))))
      

(defun test ()
  (describe (lift:run-tests)))
