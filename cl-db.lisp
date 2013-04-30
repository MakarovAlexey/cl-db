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

(defun map-class (class-name &key table-name primary-key slots)
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

(defun compute-class-mapping (class-mapping-definition class-mapping-definitions)
  (make-instance 'class-mapping
		 :mapped-class (mapped-class-of class-mapping-definition)
		 :slot-mappings (append (value-mappings-of class-mapping-definition)
					(many-to-one-mappings-of class-mapping-definition)
					(one-to-many-mappings-of class-mapping-definition))
		 :table (compute-table class-mapping-definition
				       class-mapping-definitions)))

(defun compute-table (class-mapping-definition class-mapping-definitions)
  (make-instance 'table :name (table-name-of class-mapping-definition)
		 :primary-key (primary-key-of class-mapping-definition)
		 :columns (append (loop for mapping
				     in (value-mappings-of class-mapping-definition)
				     append (columns-of mapping))
				  (loop for mapping
				     in (many-to-one-mappings-of class-mapping-definition)
				     append (columns-of mapping))
				  (loop for definition
				     in class-mapping-definitions
				     append (remove-if-not #'(lambda (mapping)
							       (eq (mapped-class-of mapping)
								   (mapped-class-of class-mapping-definition)))
							   (one-to-many-mappings-of definition))))))

;; наследование
;; в т.ч. проверка одинаковости первичного ключа и родительских классов