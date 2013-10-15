(in-package #:cl-db)

(defclass expression ()
  ((expression-type :initarg :expression-type
		    :reader expression-type-of)
   (arguments :initarg :arguments
	      :reader arguments-of)))

(defclass sql-operator ()
  ())

(defclass sql-function ()
  ((name :initarg :name :reader name-of)
   (sql-name :initarg :sql-name :reader sql-name-of)))

