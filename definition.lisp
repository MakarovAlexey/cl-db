(in-package #:cl-db)

(defvar *mappings* (make-hash-table))

(defmacro define-mapping (name)
  `(setf (gethash *mappings* ,name) (list)))

(defvar *mapping*)

(defun use-mapping (name)
  (setf *mapping* (gethash *mappings* name)))

;;(defmacro define-class-mappings ((class-name table-name) options
;;				 &rest slot-mapppings)
;;  `(setf (gethash 
;;  (let ((slot-mappings (mapcar #'(lambda (slot-mapping)
;;				   (destructuring-bind (slot-name (
;;								   (appply #'compute-slot-mapping-definition))
;;								  slot-mapppings)))
;;    `(make-instance 'class-mapping-definition
;;		    :mapped-class (find-class (quote ,class-name))
;;		    :table-name ,table-name
;;		    :primary-key (list* ,primary-key-column ,primary-key-columns)
;;		    :slot-mappings ,(loop for (slot-name (mapping-type &rest args) marshaller unmarshaller)
;;					 (case 
;;					     (compute-slot-mapping-definition mapping-type slot-name marshaller unmarshaller
;;									      dolist (mapping slot-mapppings)
;;									      (destruc

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (value-mappings :initarg :value-mappings
		   :reader value-mappings-of)
   (many-to-one-mappings :initarg :many-to-one-mappings
			 :reader many-to-one-mappings-of)
   (one-to-many-mappings :initarg :one-to-many-mappings
			 :reader one-to-many-mappings-of)))

(defclass root-class-mapping-definition
    (class-mapping-definition)
  ((primary-key :initarg :primary-key
		:reader primary-key-of)))

(defclass subclass-mapping-definition
    (class-mapping-definition)
  ((mapped-superclasses :initarg :mapped-superclasses
			:reader mapped-superclasses-of)))

(defmethod initialize-instance :after ((instance class-mapping-definition)
				       &key many-to-one-mappings
				       one-to-many-mappings)
  (dolist (mapping (append many-to-one-mappings one-to-many-mappings))
    (setf (class-mapping-definition-of mapping) instance)))

(defun value-mapping-p (mapping)
  (typep mapping (find-class 'value-mapping-definition)))

(defun many-to-one-mapping-p (mapping)
  (typep mapping (find-class 'many-to-one-mapping-definition)))

(defun one-to-many-mapping-p (mapping)
  (typep mapping (find-class 'one-to-many-mapping-definition)))

(defun map-class (class-name table-name primary-key &rest slots)
  (make-instance 'root-class-mapping-definition
		 :table-name table-name
		 :primary-key primary-key
		 :mapped-class (find-class class-name)
		 :value-mappings (remove-if-not #'value-mapping-p slots)
		 :many-to-one-mappings (remove-if-not #'many-to-one-mapping-p slots)
		 :one-to-many-mappings (remove-if-not #'one-to-many-mapping-p slots)))

;;(defun superclass (class-name &rest primary-key)

(defun map-subclass (class-name superclasses table-name &rest slots)
  (make-instance 'subclass-mapping-definition
		 :table-name table-name
		 :mapped-class (find-class class-name)
		 :mapped-superclasses (mapcar #'find-class superclasses)
		 :value-mappings (remove-if-not #'value-mapping-p slots)
		 :many-to-one-mappings (remove-if-not #'many-to-one-mapping-p slots)
		 :one-to-many-mappings (remove-if-not #'one-to-many-mapping-p slots)))

(defclass slot-mapping-definition ()
  ((class-mapping-definition :accessor class-mapping-definition-of)
   (slot-name :initarg :slot-name :reader slot-name-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)
   (marshaller :initarg :marshaller :reader marshaller-of)))

(defclass value-mapping-definition (slot-mapping-definition)
  ((columns-definitions :initarg :columns-definitions
			:reader columns-definitions-of)))

(defclass reference-mapping-definition (slot-mapping-definition)
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defclass one-to-many-mapping-definition
    (reference-mapping-definition)
  ())

(defclass many-to-one-mapping-definition
    (reference-mapping-definition)
 ())

(defun map-slot (slot-name mapping
		 &optional (marshaller #'list) (unmarshaller #'list))
  (funcall mapping slot-name marshaller unmarshaller))

(defclass column-definition ()
  ((name :initarg :name :reader name-of)
   (type-name :initarg :type-name :reader type-name-of)))

(defun column (name type-name)
  (make-instance 'column-definition :name name :type-name type-name))

(defun value (column &rest columns)
  #'(lambda (slot-name marshaller unmarshaller)
      (make-instance 'value-mapping-definition
		     :slot-name slot-name
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :columns-definitions (list* column columns))))

(defun one-to-many (class-name column-name &rest column-names)
  #'(lambda (slot-name marshaller unmarshaller)
      (make-instance 'one-to-many-mapping-definition
		     :slot-name slot-name
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :mapped-class (find-class class-name)
		     :columns-names (list* column-name column-names))))

(defun many-to-one (class-name column-name &rest column-names)
  #'(lambda (slot-name marshaller unmarshaller)
      (make-instance 'many-to-one-mapping-definition
		     :slot-name slot-name
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :mapped-class (find-class class-name)
		     :columns-names (list* column-name column-names))))