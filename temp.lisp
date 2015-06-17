;;;; cl-db.lisp

(in-package #:cl-db)

;; использовать классы *-mapping для строгого конфигурирования и нестрогого
;; или сделать конфигурирование в функциональном стиле

;;(defclass persistence-unit ()
;;  ((direct-mappings :initarg :mappings :reader effective-mappings-of)
;;   (tables :initarg :tables :reader tables-of)
;;   (effective-mappings :initarg :mappings :reader effective-mappings-of)))

(defclass class-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (superclasses :initarg :superclasses :reader superclasses-of)
   (slot-mappings :initarg :slot-mappings :reader slot-mappings-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (table-name :initarg :table-name :reader table-name-of)))

(defclass column ()
  ((name :initarg :name :reader name-of)))

(defclass table ()
  ((name :initarg :name :reader name-of)
   (columns :initarg :columns :reader columns-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (foreign-keys :initarg :foreign-keys :reader foreign-keys-of)))

(defclass effective-class-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (superclasses-names :initarg :superclasses :reader superclasses-of)
   (slot-mappings :initarg :slot-mappings :reader slot-mappings-of)
   (table :initarg :table :reader table-of)))

(defclass direct-slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (direct-mapping :initarg :mapping :reader direct-mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defclass effective-slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (effective-mapping :initarg :mapping :reader effective-mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defclass direct-value-mapping ()
  ((columns-names :initarg :columns-names :reader columns-names-of)))

(defclass effective-value-mapping ()
  ((columns :initarg :columns :reader columns-of)))

(defclass direct-one-to-many-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns-names :initarg :columns-names :reader columns-names-of)))

(defclass effective-one-to-many-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns :initarg :columns :reader columns-of)))

(defclass direct-many-to-one-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns-names :initarg :columns-names :reader columns-names-of)))

(defclass effective-many-to-one-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns :initarg :columns :reader columns-of)))

(defun map-class (class-name &key table-name primary-key superclasses slots)
  (make-instance 'class-mapping
		 :class-name class-name
		 :superclasses superclasses
		 :slot-mappings slots
		 :table-name table-name
		 :primary-key primary-key))

(defun make-persistence-unit (&rest class-mappings)
  (reduce #'compute-mapping class-mappings
	  :initial-value (make-instance 'persistence-unit)))

;(defun compute-mapping (persistence-unit class-mapping)
;  (make-instance 'persistence-unit
;		 
	;	 :tables (compute-tables (tables-of persistence-unit))
	;	 :class-mappings
		 ;;(lambda (persistence-unit class-mapping)
	      ;(make-instance 'persistence-unit :tables

;;(defun compute-tables (class-mappings)
;;  (reduce #'(lambda (tables class-mapping)
;;	      (let ((table-name (table-name-of class-mapping))
;;		    (foreign-keys (compute-foreign-keys (class-name-of class-mapping)
;;							class-mappings)))
;;		(setf (gethash table-name tables)
;;		      (make-instance 'table
;;				     :name table-name
;;				     :foregn-keys foreign-keys
;;				     :columns (append (reduce #'append
;;							      (mapcar #'columns-of
;;								      (value-mappings-of class-mapping)))
;;						      (reduce #'append
;;							      (mapcar #'columns-of foreign-keys)))))))))

;;compute-columns table-name mappings)))))))

;;(defun compute-columns (table-name class-mappings)
;;  (reduce #'(lambda (columns slot-mapping-definition)
	      




(defun map-slot (slot-name mapping &optional constructor reader)
  (make-instance 'slot-mapping :slot-name slot-name
		 :mapping mapping
		 :constructor serializer
		 :reader deserializer))

(defun value (&rest columns)
  (make-instance 'value-mapping :columns columns))

(defun one-to-many (class-name &rest columns)
  (make-instance 'one-to-many :class-name class-name :columns columns))

(defun many-to-one (class-name &rest columns)
  (make-instance 'many-to-one :class-name class-name :columns columns))

(defun column (name) ;;? может для определения типа столбца
  (make-instance 'column :name name))

(defclass clos-session ()
  ((mappings :initarg :mappings :reader mappings-of)
   (loaded-objects :initarg :loaded-objects :reader loaded-objects-of)))

(defun make-clos-session (connector &rest class-mappings)
  (make-instance 'clos-session
		 :connection (funcall connector)
		 :cache (make-hash-table :size (length class-mappings))
		 :mappings (reduce #'(lambda (table class-mapping)
				       (setf (gethash (class-name-of class-mapping) table)
					     class-mapping)
				       table)
				   class-mappings
				   :initial-value (make-hash-table :size (length class-mappings)))))

(defvar *session*)

(defun db-read (&key all)
  (db-find-all all))

(defun db-find-all (session class-name)
  (let ((class-mapping (gethash class-name (mappings-of session))))
    (map 'list #'(lambda (row)
		   (load (reduce #'(lambda (table class-mapping)
				       (setf (gethash (class-name-of class-mapping) table)
					     class-mapping)
				       table)
				   class-mappings
				   :initial-value (make-hash-table :size (length class-mappings)))))

(defvar *session*)

(defun db-read (&key all)
  (db-find-all all))

(defun db-find-all (session class-name)
  (let ((class-mapping (gethash class-name (mappings-of session))))
    (map 'list #'(lambda (row)
		   (load row class-mapping session))
	 (execute (connection-of session)
		  (make-select-query (table-of class-mapping))))))

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

;;(with-session (*session*)
;;  (db-read :all 'project))

(defgeneric write-expression (expression-type &rest args)
  )

(defun write-select-list (&reat args)
  #'(lambda (stream)
      (format stream "~{~\write-expression~\~^~%}" args)))

(defun write-query (stream &key 

(defun write-expression (stream expression-type &rest args)
  (apply (case expression-type
	   (:select #'write-select-list)
	   (:label  #'write-label)
	   (:column #'write-column)
	   (:from #'write-from-clause)
	   (:inner-join #'write-inner-join)
	   (:left-join #'write-left-join)
	   (:cross-join #'write-cross-join)
	   (:where #'write-where-clause)
	   (:and #'write-and-clause)
	   (:or #'write-or-clause)
	   (:not #'write-not-clause)
	   (:between #'write-between-clause)
	   (:count #'write-count-clause)
	   (:avg #'write-avg-clause)
	   (:sum #'write-sum-clause)
	   (:< #'write-less-than-clause)
	   (:> #'write-more-than-clause)
	   (:<= #'write-less-than-or-eq-clause)
	   (:>= #'write-more-than-or-eq-clause)
	   (:= #'write-equal-clause)
	   (:<> #'write-not-equal)
	   (:is #'write-is-clause)
	   (:null #'write-null-value)
	   (:true #'write-true-value)
	   (:false #'write-false-value)
	   :between :like :every :max :min)
	 args))

(defun postgresql-9.4 (query-expression)
  (with-open-stream (stream (make-string-output-stream))
    (dolist (expression query-expression)
      (apply #'write-expression stream query-expression))))

(in-package :yacc)

(defun list-lexer (list)
  #'(lambda ()
      (values-list (pop list))))

;; select-item := columns | column | function | operator, from-clause
;; from-clause := table-reference | left-join | inner-join
;; where-clause :=     
(defparameter *expression*
  '((function "count")
    (column "project_id")
    (table "project_members_1")
    (column "user_id")
    (table "project_members_1")
    (label "op_11")
    (from "project_members")
    (label "project_members_1")))

(define-parser *sql*
  (:start-symbol sql-query)
  (:terminals (query label select function column table from))

  (sql-query
   (select-list from-clause))

  (select-list
   (select-item select-list)
   nil)

  (select-item
   (column-reference label)
   (function-call label))

  (column-reference
   (column table))

  (function-call
   (function arguments))

  (arguments
   (column-reference arguments)
   nil)

  (from-clause
   (from label)))
