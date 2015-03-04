(in-package #:cl-db)

(defvar *transaction*)

(defclass clos-transaction ()
  ((session :initarg :session
	    :reader session-of)
   (object-snapshots :initarg :snapshots
		     :reader object-snapshots-of)
   (new-objects :initform (list)
		:accessor new-objects-of)
   (removed-objects :initform (list)
		    :accessor removed-objects-of)))

;; implement cascade operations
(defun persist-object (object &optional (session *session*))
  (pushnew object (new-objects-of session)))

(defun remove-object (object &optional (session *session*))
  (pushnew object (removed-objects-of session)))

(defclass object-map ()
  ((object :initarg :object
	   :reader object-of)
   (mapping :initarg :mapping
	    :reader mapping-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (properties :initarg :properties
	       :reader properties-of)
   (many-to-one :initarg :many-to-one
		:reader many-to-one-of)
   (one-to-many :initarg :one-to-many
		:reader one-to-many)
   (inverted-one-to-many :initform (list)
			 :accessor inverted-one-to-many-of
			 :documentation
			 "Many-to-one side of other one-to-many relations")))

(defun map-properties (object properties)
  (reduce #'(lambda (result property)
	      (destructuring-bind (slot-name column-name column-type)
		  property
		(declare (ignore column-name column-type))
		(acons slot-name (slot-value object slot-name) result)))
	  properties))

(defun map-many-to-one-associations (object many-to-one-mappings)
  )

(defun map-object (object primary-key-value &rest mapping
		   &key properties one-to-many-mappings
		     many-to-one-mappings &allow-other-keys)
  (make-instance 'object-map
		 :object object
		 :mapping mapping
		 :primary-key primary-key-value
		 :properties
		 (map-properties object properties)
		 :many-to-one
		 (map-many-to-one-associations object many-to-one-mappings)
		 :one-to-many
		 (map-one-to-many-associations object one-to-many-mappings)))

(defun map-objects (objects-table mapping-schema)
  (let ((maps (make-hash-table :size (hash-table-size objects-table))))
    (maphash #'(lambda (pk-and-class-name object)
		 (setf (gethash object maps)
		       (apply #'map-object
			      object
			      (rest pk-and-class-name)
			      (list* :class-name
				     (get-class-mapping (class-name
							 (class-of object))
							mapping-schema)))))
	     objects-table)
    maps))

(defun begin-transaction (session)
  (execute "BEGIN" (connection-of session))
  (make-instance 'clos-transaction
		 :snapshots (map-objects
			     (loaded-objects-of session)
			     (mapping-schema-of session))))

(defun rollback (transaction)
  (execute "ROLLBACK" (connection-of
		       (session-of transaction))))

(defun commit (transaction)
  (execute "COMMIT" (connection-of
		     (session-of transaction))))

(defun call-with-transaction (session thunk)
  (let ((*transaction* (begin-transaction session)))
    (funcall thunk)
    (commit *transaction*)))

(defmacro with-transaction ((&key (session *session*)) &body body)
  `(apply #'call-with-transaction
	  ,session #'(lambda () ,@body)))
