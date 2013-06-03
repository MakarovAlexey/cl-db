(in-package #:cl-db)

(defclass schema ()
  ((tables :initarg :tables :reader tables-of)))

(defclass table ()
  ((name :reader name-of)
   (columns :reader columns-of)
   (primary-key :reader primary-key-of)
   (foreign-keys :reader foreign-keys-of)))

(defclass column ()
  ((name :initarg :name :reader name-of)
   (type :initarg :type :reader column-type-of)))

(defclass foreign-key ()
  ((table :initarg :table :reader table-of)
   (columns :initarg :columns :reader columns-of)))

(defun table (schema name primary-key &rest columns)
  (setf (gethash name (tables-of schema))
	(make-instance 'table :name name
		       :primary-key primary-key
		       :columns (alexandria:alist-hash-table
				 (mapcar #'(lambda (column)
					     (cons (name-of column)
						   column))
					 columns)))))

(defun column (name type)
  (make-instance 'column :name name :type type))

(defun foreign-key (schema name table-name
		    referenced-table-name &rest columns)
  (let ((table (get-table table name schema))
	(foreign-key (make-instance 'foreign-key
				    :name name :table table
				    :referenced-table (get-table referenced-table-name schema)
				    :columns (mapcar #'(lambda (name)
							 (get-column name table))
						     columns))))
    (setf (gethash name (foreign-keys-of schema))
	  foreign-key)))
    (setf (gethash name (foreign-keys-of schema))
	  foreign-key)))

;;(defmethod shared-initialize :after ((instance table) slot-names
;;				     &key columns primary-key foreign-keys)
;;  (with-slots (columns) instance
;;    (setf columns (reduce #'(lambda (alist column)
;;			      (acons (name-of column) column alist)) columns))))
;;  (let ((computed-data-tables (compute-data-tables direct-class-mappings)))
;;    (with-slots (tables) instance ;;effective-class-mappings)
;;	(setf tables computed-data-tables))))