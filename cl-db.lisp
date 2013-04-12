;;;; cl-db.lisp

(in-package #:cl-db)

(defclass class-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (table-name :initarg :table-name :reader table-name-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (superclasses :initarg :superclasses :reader superclasses-of)
   (slot-mappings :initarg :slot-mappings :reader slot-mappings-of)))

(defun map-class (class-name &key table-name primary-key slots superclasses)
  (make-instance 'class-mapping
		 :class-name class-name
		 :table-name table-name
		 :primary-key primary-key
		 :slot-mappings slots
		 :superclasses superclasses))

(defclass slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (mapping :initarg :mapping :reader mapping-of)
   (deserializer :initarg :deserializer :reader deserializer-of)
   (serializer :initarg :serializer :reader serializer-of)))

(defun map-slot (slot-name mapping &optional deserializer serializer)
  (make-instance 'slot-mapping :slot-name slot-name
		 :mapping mapping
		 :serializer serializer
		 :deserializer deserializer))

(defclass column ()
  ((name :initarg :name :reader name-of)))

(defun column (name)
  (make-instance 'column :name name))

(defclass value-mapping ()
  ((columns :initarg :columns :reader columns-of)))

(defun value (&rest columns)
  (make-instance 'value-mapping :columns columns))

(defclass one-to-many ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns :initarg :columns :reader columns-of)))

(defun one-to-many (class-name &rest columns)
  (make-instance 'one-to-many :class-name class-name :columns columns))

(defclass one-to-many ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns :initarg :columns :reader columns-of)))

(defun many-to-one (class-name &rest columns)
  (make-instance 'many-to-one :class-name class-name :columns columns))

(defclass clos-session ()
  ((mappings :initarg :mappings :reader mappings-of)
   (loaded-objects :initarg :loaded-objects :reader loaded-objects-of)))

(defun make-clos-session (connector &rest class-mappings)
  (make-instance 'clos-session
		 :connection (funcall connectorxs)
		 :cache (make-hash-table :size (length class-mappings))
		 :mappings (reduce #'(lambda (table class-mapping)
				       (setf (gethash (class-name-of class-mapping) table)
					     class-mapping)
				       table)
				   class-mappings
				   :initial-value (make-hash-table :size (length class-mappings)))))

(defun db-read (&key all)
  (db-find-all all))

(defun db-find-all (session class-name)
  (let ((class-mapping (gethash class-name (mappings-of session))))
    (map 'list #'(lambda (row)
		   (load class-mapping row))
	 (execute (connection-of session) (make-select-query class-mapping)))))

(defun make-select-query (table)
  (let ((table-name (name-of table)))
    (format nil "SELECT ~{~a~^, ~} FROM ~a"
	    (map 'list #'(lambda (column)
			   (format nil "~a.~a" table-name (name-of column)))
		 (columns-of table)) table-name)))

(defclass clos-transaction ()
  ((clos-session :initarg :clos-session :reader clos-session-of)
   (new-objects :initform (list) :accessor new-objects-of)
   (objects-snapshots :initarg :objects-snapshots :reader object-snapshots)))

;;(defun begin-transaction (&optional (session *session*))
;;  (make-instance 'clos-transaction :clos-session clos-session
;;		 :objects-snapshots (make-snapshot (list-loaded-objects session)
;;						   (mappings-of session))))

;;(defun list-loaded-objects (session)
  

;;(defun persist (object &optional (transaction *transaction*))
;;  (when (not (loaded-p object (clos-session-of transaction)))
;;    (push (new-objects-of transaction) object)))



;;(defgeneric db-query (connection query))

;;(defun load (mapping row))

;;  (query-db (connection-of session)
;;	    (get-mapping class-name)

;;(defun db-get (class-name &rest primary-key)

;;

;;(with-session (*session*)
;;  (db-read :all 'project))