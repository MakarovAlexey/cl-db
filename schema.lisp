(in-package #:cl-db)

(defgeneric create-script (object))

(defmethod create-script ((instance mapping-schema))
  (mapcar #'create-script (tables-of instance)))

(defmethod create-script ((instance table))
  (format nil "CREATE TABLE ~a (~% ~{ ~a~^,~% ~}~%);"
	  (name-of instance)
	  (mapcar #'create-script (columns-of instance))))

(defmethod create-script ((instance column))
  (format nil "~a ~a"
	  (name-of instance)
	  (sql-type-of instance)))