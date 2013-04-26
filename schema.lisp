(in-package #:cl-db)

(defclass column ()
  ((name :initarg :name :reader name-of)
   (type :initarg :type :reader column-type-of)))

(defclass foreign-key ()
  ((table :initarg :table :reader table-of)
   (columns :initarg :columns :reader columns-of)))

(defclass table ()
  ((name :reader name-of)
   (columns :reader columns-of)
   (primary-key :reader primary-key-of)
   (foreign-keys :reader foreign-keys-of)))

;;(defmethod shared-initialize :after ((instance table) slot-names
;;				     &key columns primary-key foreign-keys)
;;  (with-slots (columns) instance
;;    (setf columns (reduce #'(lambda (alist column)
;;			      (acons (name-of column) column alist)) columns))))
;;  (let ((computed-data-tables (compute-data-tables direct-class-mappings)))
;;    (with-slots (tables) instance ;;effective-class-mappings)
;;	(setf tables computed-data-tables))))