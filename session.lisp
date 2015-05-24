(in-package #:cl-db)

(defvar *session*)

(defvar *flush-states*)

(defvar *state-path*)

(defvar *defered-updates*)

(defvar *flushed-states*)

(defgeneric execute-query (connection sql-string))

(defgeneric prepare-query (connection name sql-string))

(defgeneric execute-prepared (connection name &rest parameters))

(defgeneric close-connection (connection))

(defclass clos-session ()
  ((connection :initarg :connection
	       :reader connection-of)
   (mappping-schema :initarg :mapping-schema
		    :reader mapping-schema-of)
   (loaded-objects :initform (make-hash-table :test #'equal)
		   :reader loaded-objects-of
		   :documentation
		   "Objects by mapping name and primary key (list* class-name primary-key)")
   (commited-states :initform (make-hash-table :test #'equal)
		    :reader instance-states-of
		    :documentation
		    "Instance commited states by mapping name and object (list class-name object)")
   (new-objects :initform (list)
		:accessor new-objects-of)
   (removed-objects :initform (list)
		    :accessor removed-objects-of)))

(defclass instance-state ()
  ((object :initarg :object
	   :reader object-of)
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (property-values :initarg :property-values
		    :accessor property-values-of)
   (dependencies :initarg :dependencies
		 :initform (list)
		 :accessor dependencies-of)))

(defclass commited-state (instance-state)
  ((table-row :initarg :table-row
	      :reader table-row-of
	      :documentation "alist of values by column names")))

(defclass flush-state (instance-state)
  ())

(defclass new-instance (flush-state)
  ())

(defclass state-diff (flush-state)
  ((commited-state :initarg :commited-state
		   :reader commited-state-of)))

(defclass removed-state (state-diff)
  ())

(defclass state-dependency ()
  ((dependency-state :initarg :state
		     :reader dependency-state-of)
   (dependency-mapping :initarg :dependency-mapping
		       :reader dependency-mapping-of)))

(defclass defered-update ()
  ((flush-state :initarg :flush-state
		:reader flush-state-of)
   (dependency :initarg :dependency
	       :reader dependency-of)))

(defun property-values (object class-mapping)
  (reduce #'(lambda (property-mapping result)
	      (let ((slot-name (slot-name-of property-mapping)))
		(if (slot-boundp object slot-name)
		    (acons property-mapping
			   (slot-value object slot-name)
			   result))))
	  (property-mappings-of class-mapping)
	  :initial-value nil
	  :from-end t))

(defun compute-superclass-dependencies (object class-mapping)
  (mapcar #'(lambda (superclass-mapping)
	      (make-instance 'state-dependency
			     :dependency-mapping superclass-mapping
			     :state
			     (ensure-state object (class-name-of superclass-mapping))))
	  (superclass-mappings-of class-mapping)))

(defun compute-many-to-one-dependency (many-to-one-mapping object
				       inverted-association)
  (let* ((many-to-one-slot-name
	  (slot-name-of many-to-one-mapping))
	 (many-to-one-object
	  (slot-value object many-to-one-slot-name))
	 (dependency
	  (make-instance 'state-dependency
			 :dependency-mapping many-to-one-mapping 
			 :state (ensure-state many-to-one-object
					      (reference-class-of many-to-one-mapping)))))
    (when (and (not (null inverted-association))
	       (not (member object (serialized-value many-to-one-object
						     inverted-association))))
      (error "Non consistent bidirectional association~%between slot ~A of object ~A~%and slot ~A of object ~A"
	      many-to-one-slot-name object
	      (slot-name-of inverted-association) many-to-one-object))
    dependency))

(defun compute-many-to-one-dependencies (object class-mapping)
  (loop for many-to-one-mapping
     in (many-to-one-mappings-of class-mapping)
     for inverted-association =
       (find-if #'(lambda (inverted-assciation)
		    (equal
		     (foreign-key-of inverted-assciation)
		     (foreign-key-of many-to-one-mapping)))
		(inverted-one-to-many-mappings-of class-mapping))
     when (slot-boundp object (slot-name-of many-to-one-mapping))
     collect (compute-many-to-one-dependency many-to-one-mapping object
					     inverted-association)))

(defun compute-existing-references (flush-state uncommited-value
				    one-to-many-mapping)
  (dolist (object uncommited-value)
    (let ((dependency-state
	   (ensure-state object (reference-class-of one-to-many-mapping))))
    (push (make-instance 'state-dependency :dependency-mapping
			 (find (slot-name-of one-to-many-mapping)
			       (inverted-one-to-many-mappings-of
				(class-mapping-of dependency-state))
			       :key #'slot-name-of)
			 :state flush-state)
	  (dependencies-of dependency-state)))))

(defun serialized-value (object one-to-many-mapping)
  (funcall (serializer-of one-to-many-mapping)
	   (slot-value object (slot-name-of one-to-many-mapping))))

(defun compute-one-to-many-dependencies (flush-state class-mapping)
  (dolist (one-to-many-mapping (one-to-many-mappings-of class-mapping))
    (compute-existing-references flush-state
				 (serialized-value (object-of flush-state)
						   one-to-many-mapping)
				 one-to-many-mapping)))

(defun compute-diff (object commited-state
		     &optional (flush-states *flush-states*))
  (let* ((class-mapping
	  (class-mapping-of commited-state))
	 (flush-state
	  (make-instance 'state-diff
			 :commited-state commited-state
			 :property-values (property-values object class-mapping))))
    (setf (gethash (list object (class-name-of class-mapping))
		   flush-states)
	  flush-state)
    (setf (superclass-dependencies-of flush-state)
	  (append
	   (compute-superclass-dependencies object class-mapping)
	   (compute-many-to-one-dependencies object class-mapping)))
    flush-state))

(defun insert-state (object class-mapping
		     &optional (flush-states *flush-states*))
  (let ((flush-state
	 (make-instance 'new-instance
			:object object
			:class-mapping class-mapping
			:property-values
			(property-values object class-mapping))))
    (setf (gethash (list object (class-name-of class-mapping))
		   flush-states)
	  flush-state)
    (setf (dependencies-of flush-state)
	  (append
	   (dependencies-of flush-state)
	   (compute-superclass-dependencies object class-mapping)
	   (compute-many-to-one-dependencies object class-mapping)))
    (compute-one-to-many-dependencies flush-state class-mapping)
    flush-state))

(defun get-subclass-mapping (object class-mapping)
  (find-if #'(lambda (class-name)
	       (typep object class-name))
	   (subclass-mappings-of class-mapping)
	   :key #'class-name-of))

(defun insert-object (object class-mapping)
  (let ((inserted
	 (insert-state object class-mapping))
	(subclass-mapping
	 (get-subclass-mapping object class-mapping)))
    (when (not (null subclass-mapping))
      (insert-object object subclass-mapping))
    inserted))

(defun compute-state (object mapping-name
		      &optional (session *session*))
  (multiple-value-bind (commited-state persistedp)
      (gethash
       (list object mapping-name)
       (instance-states-of session))
    (if (not persistedp)
	(insert-object object (get-class-mapping mapping-name))
	(compute-diff object commited-state))))

(defun get-flush-state (object mapping-name
			&optional (flush-states *flush-states*))
  (gethash (list object mapping-name) flush-states))

(defun ensure-state (object &optional (mapping-name (type-of object)))
  (or (get-flush-state object mapping-name)
      (compute-state object mapping-name)))

(defun ensure-inserted (object &optional (class-name (type-of object)))
  (let ((state
	 (get-flush-state object class-name)))
    (if (null state)
	(insert-object object (get-class-mapping class-name))
	state)))

(defun ensure-diff (commited-state &optional
				     (flush-states *flush-states*))
  (let ((object (object-of commited-state)))
    (multiple-value-bind (state presentp)
	(gethash (list object
		       (class-name-of
			(class-mapping-of commited-state)))
		 flush-states)
      (if (not presentp)
	  (compute-diff object commited-state)
	  state))))

;;(defun remove-object (commited-state)
;;  (let ((flush-state
;;	 (make-instance 'removed-instance
;;			:commited-state commited-state)))
;;    (setf (many-to-one-dependencies-of flush-state)
;;	  (mapcar #'(lambda (many-to-one-value)
;;		      (funcall (cascade-operation-of
;;				(mapping-of many-to-one-value))
;;			       flush-state
;;			       (value-of many-to-one-value)))
;;		  (many-to-one-values-of commited-state)))
;;    (setf (one-to-many-dependencies-of flush-state)
;;	  (reduce #'append (one-to-many-values-of commited-state)
;;		  :key #'(lambda (one-to-many-value)
;;			   (let ((one-to-many-mapping
;;				  (mapping-of one-to-many-commited-value)))
;;			     (funcall (cascade-operation-of one-to-many-mapping)
;;				      flush-state
;;				      (value-of one-to-many-value)
;;				      one-to-many-mapping)))))))

;;(defun ensure-removed (object &optional (mapping-name
;;					 (class-name (class-of object))))
;;  (let ((state
;;	 (get-flush-state object mapping-name)))
;;    (if (null state)
;;	(remove-object state (get-class-mapping class-name))
;;	(error "Object ~a not persisted and cannot be removed" object))))

;;; cascade removing and setting not removable object's associations
;;; to null (if null value permitted)

;; регистрируем обработанные состояния в *processed-states* (ключ -
;; состояние, значение - alist обновленных значаний по названиям
;; столбцов)

(defun process-dependencies (state state-path
			     &optional (defered-updates *defered-updates*))
  (loop for dependency in (dependencies-of state)
     for flush-state = (dependency-state-of dependency)
     if (member flush-state state-path)
     do (setf (gethash flush-state defered-updates)
	      (list* (make-instance 'defered-update
				    :flush-state state
				    :dependency dependency)
		     (gethash flush-state defered-updates)))
     else collect
       (make-instance 'state-dependency
		      :dependency-mapping (dependency-mapping-of dependency)
		      :state (process-state flush-state
					    :state-path state-path))))

(defun write-value (stream object &rest args)
  (declare (ignore args))
  (typecase object
    (string (format stream "'~A'" object))
    (t (write object :stream stream))))

(defun insert-row (table-name column-names column-values
		   &optional (session *session*))
  (execute-query
   (connection-of session)
   (format nil "INSERT INTO ~A (~{~A~^, ~})~%~5TVALUES (~{~/cl-db:write-value/~^, ~})"
	   table-name column-names column-values)))

(defun update-row (table-name primary-key foreign-key
		   &optional (session *session*))
  (execute-query
   (connection-of session)
   (format nil "UPDATE ~A~%~3TSET ~{~{~A = ~/cl-db:write-value/~}~^, ~%~7T~}~%~1TWHERE ~{~{~A = ~/cl-db:write-value/~}~^~%~3TAND ~}"
	   table-name
	   (alist-plist primary-key)
	   (alist-plist foreign-key))))

(defun row-value (property-value)
  (cons (column-of (mapping-of property-value))
	(value-of property-value)))

(defun column-value (column-name table-row)
  (rest
   (assoc column-name table-row :test #'string=)))

(defun foreign-key-values (dependency-mapping dependency-row)
  (mapcar #'(lambda (foreign-key-column primary-key-column)
	      (cons foreign-key-column
		    (column-value primary-key-column
				  dependency-row)))
	  (foreign-key-of dependency-mapping)
	  (primary-key-of
	   (get-class-mapping
	    (reference-class-of dependency-mapping)))))

(defun primary-key-values (class-mapping table-row)
  (mapcar #'(lambda (primary-key-column)
	      (cons primary-key-column
		    (column-value primary-key-column table-row)))
	  (primary-key-of class-mapping)))

(defmethod row-values (dependency)
  (let ((dependency-mapping
	 (dependency-mapping-of dependency))
	(dependency-row
	 (table-row-of
	  (dependency-state-of dependency))))
    (foreign-key-values dependency-mapping dependency-row)))

(defgeneric query-state (state defered-updates))

(defmethod query-state ((state new-instance) dependencies)
  (let* ((property-values
	  (property-values-of state))
	 (row-values
	  (append (mapcar #'row-value
			  property-values)
		  (remove-duplicates
		   (reduce #'append dependencies
			   :initial-value nil
			   :key #'row-values)
		   :test #'equal)))
	 (class-mapping (class-mapping-of state)))
    (insert-row (table-name-of class-mapping)
		(mapcar #'first row-values)
		(mapcar #'rest row-values))
    (make-instance 'commited-state
		   :table-row row-values
		   :object (object-of state)
		   :class-mapping (class-mapping-of state)
		   :property-values property-values
		   :dependencies dependencies)))

(defmethod query-state ((state state-diff) defered-updates)
  (error "Not implemented yet"))

(defmethod query-state ((state removed-state) defered-updates)
  (error "Not implemented yet"))

(defun process-defered-update (flush-state dependency-mapping
			       dependency-commited-state
			       &optional (flushed-states *flushed-states*))
  (let ((commited-state
	 (gethash flush-state flushed-states))
	(class-mapping
	 (class-mapping-of flush-state)))
    (update-row
     (table-name-of class-mapping)
     (primary-key-values class-mapping
			 (table-row-of commited-state))
     (foreign-key-values dependency-mapping
			 (table-row-of dependency-commited-state)))))

(defun process-defered-updates (flush-state commited-state
				&optional
				  (defered-updates *defered-updates*))
  (dolist (defered-update (gethash flush-state defered-updates))
    (process-defered-update (flush-state-of defered-update)
			    (dependency-mapping-of
			     (dependency-of defered-update))
			    commited-state))
  (remhash flush-state defered-updates))

(defun process-state (state &key state-path
			      (flushed-states *flushed-states*))
  (multiple-value-bind (commited-state presentp)
      (gethash state flushed-states)
    (if (not presentp)
	(let* ((state-path
		(list* state state-path))
	       (dependencies
		(process-dependencies state state-path))
	       (commited-state
		(query-state state dependencies)))
	  (setf (gethash state flushed-states) commited-state)
	  (process-defered-updates state commited-state)
	  commited-state)
	commited-state)))
    
(defun process-states (states)
  (let ((*flushed-states* (make-hash-table)))
    (dolist (state states)
      (process-state state))))

(defun flush-session (session)
  (let ((*flush-states* (make-hash-table :test #'equal))
	(*defered-updates* (make-hash-table)))
    (dolist (object (new-objects-of session))
      (ensure-inserted object))
;;    (dolist (object (removed-objects-of session))
;;      (ensure-removed object))
    (dolist (commit-state (alexandria:hash-table-values
			   (instance-states-of session)))
      (ensure-diff commit-state))
    (maphash #'(lambda (object-and-class-name flush-state)
		 (compute-one-to-many-dependencies flush-state
						   (get-class-mapping
						    (second object-and-class-name))))
	     *flush-states*)
    (process-states (hash-table-values *flush-states*))))

(defun open-session (mapping-schema-fn connection-fn)
  (make-instance 'clos-session
		 :mapping-schema (funcall mapping-schema-fn)
		 :connection (funcall connection-fn)))

(defun close-session (session)
  (flush-session session)
  (close-connection (connection-of session)))

(defun begin-transaction (&optional (session *session*))
  (execute-query (connection-of session) "BEGIN"))

(defun rollback (&optional (session *session*))
  (execute-query (connection-of session) "ROLLBACK"))

(defun commit (&optional (session *session*))
  (execute-query (connection-of session) "COMMIT"))

(defun call-with-session (mapping-schema-fn connection-fn thunk)
  (let ((*session*
	 (open-session mapping-schema-fn connection-fn)))
    (funcall thunk)
    (close-session *session*)))

(defmacro with-session ((mapping-schema connection-form)
			&body body)
  `(call-with-session
    (function ,mapping-schema)
    #'(lambda () ,connection-form)
    #'(lambda () ,@body)))

(defun db-persist (object &optional (session *session*))
  (pushnew object (new-objects-of session)))

(defun db-remove (object &optional (session *session*))
  (pushnew object (removed-objects-of session)))

(defun get-object (class-name primary-key
		   &optional (session *session*))
  (gethash
   (list* class-name primary-key)
   (loaded-objects-of session)))

(defun register-object (object class-name primary-key
			&optional (session *session*))
  (setf (gethash
	 (list* class-name primary-key)
	 (loaded-objects-of session))
	object))

(defun ensure-commited-state (object &optional (session *session*))
  (ensure-gethash object
		  (instance-states-of session)
		  (make-instance 'commited :object object)))
