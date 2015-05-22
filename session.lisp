(in-package #:cl-db)

(defvar *session*)

(defvar *flush-states*)

(defvar *state-path*)

(defvar *state-rows*)

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
   (superclass-dependencies :initarg :superclass-dependencies
			    :accessor superclass-dependencies-of)
   (many-to-one-dependencies :initarg :many-to-one-dependencies
			     :accessor many-to-one-dependencies-of)
   (one-to-many-dependencies :initarg :one-to-many-dependencies
			     :accessor one-to-many-dependencies-of)))

(defclass commited-state (instance-state)
  ((primary-key :initarg :primary-key
		:reader primary-key-of)
   (column-values :initarg :column-values
		  :reader column-values-of
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
  ((flush-state :initarg :flush-state
		:reader flush-state-of)))

(defclass superclass-dependency (state-dependency)
  ((superclass-mapping :initarg :superclass-mappings
		       :reader superclass-mapping-of)))

(defclass many-to-one-dependency (state-dependency)
  ((many-to-one-mapping :initarg :many-to-one-mapping
			:reader many-to-one-mapping-of)))

(defclass one-to-many-dependency (state-dependency)
  ((one-to-many-mapping :initarg :one-to-many-mapping
			:reader one-to-many-mapping-of)))

(defun property-values (object class-mapping)
  (reduce #'(lambda (result property-mapping)
	      (let ((slot-name (slot-name-of property-mapping)))
		(if (slot-boundp object slot-name)
		    (acons property-mapping
			   (slot-value object slot-name)
			   result))))
	  (property-mappings-of class-mapping) :initial-value nil))

(defun compute-superclass-dependencies (object class-mapping)
  (mapcar #'(lambda (superclass-mapping)
	      (make-instance 'superclass-dependency
			     :superclass-mapping superclass-mapping
			     :flush-state
			     (ensure-state object (class-name-of superclass-mapping))))
	  (superclass-mappings-of class-mapping)))

(defun compute-many-to-one-dependencies (object class-mapping)
  (mapcar #'(lambda (many-to-one-mapping)
	      (let ((slot-name (slot-name-of many-to-one-mapping)))
		(if (slot-boundp object slot-name)
		    (make-instance 'many-to-one-dependency
				   :many-to-one-mapping many-to-one-mapping 
				   :flush-state
				   (ensure-state object (referenced-class-of many-to-one-mapping))))))
	  (many-to-one-mappings-of class-mapping)))

(defun compute-existing-references (flush-state uncommited-value
				    one-to-many-mapping)
  (dolist (object uncommited-value)
    (push (make-instance 'one-to-many-dependency
			 :one-to-many-mapping one-to-many-mapping
			 :flush-state flush-state)
	  (one-to-many-dependencies-of
	   (ensure-state object (referenced-class-of one-to-many-mapping))))))

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
    (setf (gethash (list (class-name-of class-mapping) object)
		   flush-states)
	  flush-state)
    (setf (superclass-dependencies-of flush-state)
	  (compute-superclass-dependencies object class-mapping))
    (setf (many-to-one-dependencies-of flush-state)
	  (compute-many-to-one-dependencies object class-mapping))
    (compute-one-to-many-dependencies flush-state class-mapping)
    flush-state))

(defun insert-state (object class-mapping
		     &optional (flush-states *flush-states*))
  (let ((flush-state
	 (make-instance 'new-instance
			:object object
			:class-mapping class-mapping
			:property-values
			(property-values object class-mapping))))
    (setf (gethash (list (class-name-of class-mapping) object)
		   flush-states)
	  flush-state)
    (setf (superclass-dependencies-of flush-state)
	  (compute-superclass-dependencies object class-mapping))
    (setf (many-to-one-dependencies-of flush-state)
	  (compute-many-to-one-dependencies object class-mapping))
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
	(insert-object object mapping-name)
	(compute-diff object commited-state))))

(defun get-flush-state (object mapping-name
			&optional (flush-states *flush-states*))
  (gethash (list object mapping-name) flush-states))

(defun ensure-state (object &optional (mapping-name (type-of object)))
  (let ((state (get-flush-state object mapping-name)))
    (if (null state)
	(compute-state object mapping-name)
	state)))

(defun ensure-inserted (object &optional (class-name (type-of object)))
  (let ((state
	 (get-flush-state object class-name)))
    (if (null state)
	(insert-object object (get-class-mapping class-name))
	(error "Object ~a already persisted" object))))

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

;; process-state => commited-state, differed-updates
;; process-dependencies => dependencies with commited-state, differed-updates
;; superclass
;; many-to-one
;; one-to-many

(defun set-state-row (state row &optional (processed-states *state-rows*))
  (setf (gethash state processed-states) row))

(defun process-dependencies (state state-path)
  (loop for dependency
     in (append
	 (superclass-dependencies state state-path)
	 (many-to-one-dependencies state state-path)
	 (one-to-many-dependencies state state-path))
     for flush-state (flush-state-of dependency)
     when (not (find flush-state state-path))
     append (process-state flush-state))) ;; добавить differed update

(defun property-column-values (state)
  (mapcar #'(lambda (property-value)
	      (cons (column-of
		     (mapping-of property-value))
		    (values-of property-value)))
	  (property-values-of state)))

(defun process-differed-updates (state differed-updates)
  (dolist (differed-update differed-updates)
    (when (eq (differed-state-of differed-update) state)
      (process-differed-update differed-update)))
  (remove state differed-updates :key #'differed-state-of))

(defgeneric process-state (state &optional state-path))

(defmethod process-state ((state new-instance) &optional state-path)
  (let ((differed-updates
	 (process-dependencies state (list* state state-path)))
	(row
	 (append
	  (property-column-values state)
	  (foreign-key-values state))))
    (insert-query (table-name-of
		   (class-mapping-of state)) row)
    (set-state-row state row)
    (process-differed-updates state differed-updates)))

(defmethod process-state ((state state-diff) &optional state-path)
  ;; superclasses
  ;; many-to-one
  ;; appended-to
  ;; removed-from
  )

(defmethod process-state ((state removed-state) &optional state-path)
  ;; superclasses
  ;; many-to-one
  ;; appended-to
  ;; removed-from
  )

(defun process-states (states)
  (let ((*passed* (list)))
    (dolist (state states)
      (when (not (find state *passed-states*))
	(process-state state)))))

(defun flush-session (session)
  (let ((*flush-states* (make-hash-table :test #'equal)))
    (dolist (object (new-objects-of session))
      (ensure-inserted object))
;;    (dolist (object (removed-objects-of session))
;;      (ensure-removed object))
    (dolist (commit-state (alexandria:hash-table-values
			   (instance-states-of session)))
      (ensure-diff commit-state))
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
