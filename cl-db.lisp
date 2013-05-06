;;;; cl-db.lisp

(in-package #:cl-db)

;; использовать классы *-mapping для строгого конфигурирования и нестрогого
;; или сделать конфигурирование в функциональном стиле

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (value-mappings :initarg :value-mapping
		   :reader value-mappings-of)
   (many-to-one-mappings :initarg :many-to-one-mappings
			 :reader many-to-one-mappings-of)
   (one-to-many-mappings :initarg :one-to-many-mappings
			 :reader one-to-many-mappings-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (table-name :initarg :table-name
	       :reader table-name-of)))

(defun value-mapping-p (mapping)
  (typep mapping (find-class 'value-mapping-definition)))

(defun many-to-one-mapping-p (mapping)
  (typep mapping (find-class 'many-to-one-mapping-definition)))

(defun one-to-many-mapping-p (mapping)
  (typep mapping (find-class 'one-to-many-mapping-definition)))

(defun map-class (mappings class-name &key table-name primary-key slots)
  (let ((mappings (mapcar #'mapping-of slots)))
    (make-instance 'class-mapping-definition
		   :table-name table-name
		   :primary-key primary-key
		   :mapped-class (find-class class-name)
		   :value-mapping (remove-if-not #'value-mapping-p
						 mappings)
		   :many-to-one-mappings (remove-if-not #'many-to-one-mapping-p
							mappings)
		   :one-to-many-mappings (remove-if-not #'one-to-many-mapping-p
							mappings))))

(defclass slot-mapping-definition ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (mapping :initarg :mapping :reader mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defun map-slot (slot-name mapping &optional constructor reader)
  (make-instance 'slot-mapping-definition
		 :constructor constructor
		 :slot-name slot-name
		 :mapping mapping
		 :reader reader))

(defclass value-mapping-definition ()
  ((columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun value (&rest columns)
  (make-instance 'value-mapping-definition :columns-names columns))

(defclass one-to-many-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun one-to-many (class-name &rest columns)
  (make-instance 'one-to-many-mapping-definition
		 :mapped-class (find-class class-name)
		 :columns-names columns))

(defclass many-to-one-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun many-to-one (class-name &rest columns)
  (make-instance 'many-to-one-mapping-definition 
		 :mapped-class (find-class class-name)
		 :columns-names columns))

(defclass class-mapping ()
  ((table :initarg :table :reader table-of)
   (mapped-class :initarg :mapped-class :reader mapped-class-of)
   (slot-mappings :initarg :slot-mappings :reader slot-mappings-of)))

(defclass table ()
  ((name :initarg :name :reader name-of)
   (columns :initarg :columns :reader columns-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (foreign-keys :initform (list) :accessor foreign-keys-of)))

(defclass column ()
  ((name :initarg :name :reader name-of)
   (type-name :initarg :type-name :reader type-name-of)))

(defclass foreign-key ()
  ((table :initarg :table :reader table-of)
   (columns :initarg :columns :reader columns-of)))

(defclass foreign-key-column (column)
  ((foreign-keys :initarg :foreign-keys :reader foreign-keys-of)))

(defclass value-column (column)
  ((value-mapping :initarg :value-mapping :reader value-mapping-of)))

(defclass slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (mapping :initarg :mapping :reader mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defun compute-mappings (class-mapping-definitions)
  (let ((class-mappings (reduce #'(lambda (hash-table definition)
				    (let ((mapped-class (mapped-class-of definition)))
				      (setf (gethash mapped-class hash-table)
					    (make-instance 'class-mapping
							   :mapped-class mapped-class
							   :table (make-instance 'table
										 :name (table-name-of definition)
										 :primary-key (primary-key-of class-mapping-definition)))))
				    hash-table)
				class-mapping-definitions
				:initial-value (make-hash-table :size (length class-mapping-definitions)))))
    (dolist (definition class-mapping-definitions class-mappings)
      (setf (slot-value (gethash (mapped-class-of definition) class-mappings) 'slots-mappings)
	    (mapcar #'(lambda (definition)
			(make-instance 'slot-mapping
				       :slot-name (slot-name-of definition)
				       :mapping (compute-mapping (mapping-of definition)
								 definition class-mappings)))
		    (slot-mappings-of definition))))))

(defgeneric compute-mapping (mapping-definition class-mapping
			     class-mapping-definitions))

(defclass value-mapping ()
  ((columns :initarg :columns :reader columns-of)))

(defun column-exist-p (table column-name)
  (gethash name (columns-of table)))

(defun add-column (table column)
  (setf (gethash name (columns-of table)) column))

(defmethod make-value-column (value-mapping table name)
  (if (not (column-exist-p table name))
      (add-column table (make-instance 'value-column
				       :name name :table table))
      (error "Duplicte mappig of column ~a" name)))

	

(defmethod compute-slot-mapping ((mapping-definition value-mapping-definition)
				 class-mapping class-mapping-definitions)
  (declare (ignore class-mapping-definitions))
  (make-instance 'value-mapping
		 :columns (mapcar #'(lambda (column-name)
				      (make-value-column column-name
						  (table-of class-mappings-definition)))
				  (columns-names-of mapping-definition))))

(defclass one-to-many-mapping ()
  ((class-mapping :initarg :class-mapping :reader class-mapping-of)
   (foreign-key :initarg :foreign-key :reader foreign-key-of)))

(defun get-mapping (mapped-class class-mapping-definitions)
  (gethash mapped-class class-mapping-definitions))

(defun ensure-foreign-key (table referenced-table columns)
  
  

(defmethod compute-slot-mapping ((mapping-definition one-to-many-mapping-definition)
				 class-mapping class-mappings-definitions)
  (declare (ignore class-mapping))
  (let ((class-mapping (get-mapping (mapped-class-of mapping-definition)
				    class-mapping-definitions)))
    (make-instance 'one-to-many-mapping :class-mapping class-mapping
		   :foreign-key (ensure-foreign-key (table-of (get-class-mapping
							       (mapped-class-of mapping-definition)
							       class-mapping-definitions))
						    (columns-names-of mapping-definition)
						    (table-of class-mapping)))))

(defclass many-to-one-mapping ()
  ((class-mapping :initarg :class-mapping :reader class-mapping-of)
   (foreign-key :initarg :foreign-key :reader foreign-key-of)))

(defmethod compute-slot-mapping ((mapping-definition many-to-one-mapping-definition)
				 class-mapping class-mapping-definitions)
  (make-instance 'many-to-one-mapping :class-mapping class-mapping
		 :foreign-key (enure-foreign-key (table-of class-mapping)
						 (table-of (get-class-mapping (mapped-class-of mapping-definition)
									      class-mapping-definitions))
						 (columns-names-of mapping-definition))))

(defun compute-schema (class-mapping-definitions)
  (let ((tables (mapcar #'(lambda (class-mapping-definition)
			    (cons class-mapping-definition
				  (compute-table class-mapping-definition
						 class-mapping-definitions)))
			class-mapping-definitions)))
;; наследование
;; в т.ч. проверка одинаковости первичного ключа и родительских классовx