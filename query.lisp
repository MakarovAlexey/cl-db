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
	  :initial-value (apply #'plan-query query-plan path)))

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

;;(defmethod compute-select-item ((select-item lisp) query-plan &rest args))

(defun get-plan-node (binding query-plan)
  (reduce #'(lambda (nodes key)
	      (assoc key nodes))
	  (binding-path select-item)
	  :initial-value query-plan
	  :key )

;;  (let ((path (binding-path select-item)))
;;    (reduce #'(lambda (plan node)
;;		(find node (rest plan) :key #'first))
;;	    (rest path)
;;	    :initial-value (find (first path) query-plan
;;				 :key #'first))))

(defmethod compute-clause-item (query-plan
				(select-item root-binding) &rest args)
  (list
   (class-mapping-of select-item)
   (assoc key query-plan)))

(defmethod compute-clause-item (query-plan
				(select-item reference-binding) &rest args)
  (list
   (refrenced-class-mapping-of
    (reference-mapping-of select-item))
   (get-plan-node select-item)))

(defmethod compute-clause-item (query-plan
				(select-item value-binding) &rest args)
  (list
   (value-mapping-of select-item)
   (get-plan-node (parent-mapping-of select-item))))

(defmethod compute-clause-item (query-plan
				(select-item expression) &rest args)
  (list* select-item (mapcar #'compute-select-item args)))

(defun compute-clause (clause query-plan)
  (mapcar #'(lambda (clause-item)
		      (apply #'compute-clause-item
			     query-plan clause-item)) clause))

(defun make-main-query (select-list where order-by having
			limit offset single query-plan)
  (list :select-list (compute-clause query-plan select-list)
	:from-clause query-plan
	:where-clause (compute-clause query-plan where)
	:order-by-clause (compute-clause order-by)
	:having-clause (compute-clause having)
	:limit limit
	:offset offset))

;; TODO fetch and single
(defun make-query (select-list &key where order-by having
		   fetch-also limit offset single)
  (let ((main-query
	 (make-main-query select-list where order-by
			  having limit offset single
			  (reduce #'plan-clause
				  (append where order-by having)
				  :initial-value
				  (reduce #'plan-select-item
					  select-list
					  :initial-value nil)))))
    (if (not (null fetch-also))
	(reduce #'plan-fetch-also-clause fetch-also
		:initial-value )
	main-query)))

;;(defgeneric load-row (loader row))