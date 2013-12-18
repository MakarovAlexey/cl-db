(in-package #:cl-db)

(defclass query ()
  ((query-plan :initform (list)
	       :initarg :query-plan
	       :reader query-plan-of)
   (select-clause :initform (list)
		  :initarg :select-clause
		  :reader select-clause-of)
   (where-clause :initform (list)
		 :initarg :where-clause
		 :reader where-clause-of)
   (order-by-clause :initform (list)
		    :initarg :order-by-clause
		    :reader order-by-clause-of)
   (having-clause :initform (list)
		  :initarg :having-cluse
		  :reader having-clause-of)
   (limit :reader limit-of)
   (offset :reader offset-of)
   (single-instance :reader single-instance-p)))

(defun plan-query (children node &rest path)
  (list*
   (list* node
	  (if (not (null path))
	      (apply #'plan-query
		     (rest (find node children :key #'first)) path)
	      (rest (find node children :key #'first))))
   (remove node children :key #'first)))

(defgeneric binding-path (binding))

(defmethod binding-path ((binding root-binding))
  (list binding))

(defmethod binding-path ((binding reference-binding))
  (reverse
   (list* binding
	  (reverse
	   (append
	    (binding-path (parent-binding-of binding))
	    (path-of (reference-mapping-of binding)))))))

(defun plan-inheritance (query-plan class-mapping &rest path)
  (reduce #'(lambda (query-plan inheritance-mapping)
	      (apply #'plan-inheritance query-plan
		     (superclass-mapping-of inheritance-mapping)
		     (list* inheritance-mapping path)))
	  (inheritance-mappings-of class-mapping)
	  :initial-value (apply #'plan-query query-plan path)))

(defun plan-extension (query-plan class-mapping &rest path)
  (apply #'plan-query
	 (reduce #'(lambda (query-plan extension-mapping)
		     (let ((new-path (reverse
				      (list* extension-mapping
					     (reverse path)))))
		       (apply #'plan-extension
			      (apply #'plan-inheritance query-plan
				     class-mapping
				     new-path)
			      (subclass-mapping-of extension-mapping)
			      new-path)))
		 (extension-mappings-of class-mapping)
		 :initial-value (apply #'plan-query query-plan path))
	 path))

(defgeneric plan-select-item (query-plan select-item))

(defmethod plan-select-item (query-plan (select-item root-binding))
  (let ((class-mapping (class-mapping-of select-item))
	(path (binding-path select-item)))
    (apply #'plan-extension
	   (apply #'plan-inheritance
		  (plan-query query-plan select-item) class-mapping path)
	   class-mapping path)))

(defmethod plan-select-item (query-plan (select-item reference-binding))
  (let ((class-mapping
	 (referenced-class-mapping-of (reference-mapping-of select-item)))
	(path (binding-path select-item)))
    (apply #'plan-extension
	   (apply #'plan-inheritance query-plan class-mapping path)
	   class-mapping path)))

(defmethod plan-select-item (query-plan (select-item value-binding))
  (apply #'plan-query query-plan
	 (binding-path
	  (parent-binding-of select-item))))

(defmethod plan-select-item (query-plan (select-item expression))
  (reduce #'plan-select-item (arguments-of select-item)
	  :initial-value query-plan))

(defgeneric plan-clause (query-plan clause))

(defmethod plan-clause (query-plan (clause root-binding))
  (plan-query query-plan clause))

(defmethod plan-clause (query-plan (clause reference-binding))
  (apply #'plan-query query-plan (binding-path clause)))

(defmethod plan-clause (query-plan (clause value-binding))
  (apply #'plan-query query-plan
	 (binding-path
	  (parent-binding-of clause))))

(defmethod plan-clause (query-plan (clause expression))
  (reduce #'plan-clause (arguments-of clause)
	  :initial-value query-plan))

(defun make-query (select-list &key where order-by having
		   fetch-also limit offset single)
;;  (reduce #'plan-fetch-also-clause fetch-also
;;	  :initial-value
	  (reduce #'plan-clause (append where order-by having)
		  :initial-value
		  (reduce #'plan-select-item select-list
			  :initial-value nil)))