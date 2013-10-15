(in-package #:cl-db)

(defclass root-binding ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)))

(defclass reference-binding ()
  ((parent-binding :initarg :parent-binding
		   :reader parent-binding-of)
   (reference-mapping :initarg :reference-mapping
		      :reader reference-mapping-of)))

(defmethod class-mapping-of ((object reference-binding))
  (class-mapping-of (reference-mapping-of object)))

(defclass value-binding ()
  ((parent-binding :initarg :parent-binding
		   :reader parent-binding-of)
   (value-mapping :initarg :value-mapping
		  :reader value-mapping-of)))

(defun bind-root (class-name &optional
		  (mapping-schema *mapping-schema*))
  (make-instance 'root-binding
		 :class-mapping (get-class-mapping
				 (find-class class-name)
				 mapping-schema)))

(defun bind-reference (parent-binding accessor)
  (make-instance 'reference-binding
		 :parent-binding parent-binding
		 :reference-mapping (get-reference-mapping
				     (class-mapping-of parent-binding)
				     accessor)))

(defun bind-value (parent-binding accessor)
  (make-instance 'value-binding
		 :parent-binding parent-binding
		 :value-mapping (get-value-mapping
				 (class-mapping-of parent-binding)
				 accessor)))
