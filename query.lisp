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

;; допилить план наследования
(defun plan-inheritance (query-plan class-mapping &rest path)
  (reduce #'(lambda (query-plan inheritance-mapping)
	      (let ((new-path (reverse
			       (list* inheritance-mapping
				      (reverse path)))))
		(apply #'plan-inheritance query-plan
		       (superclass-mapping-of inheritance-mapping)
		       new-path)))
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

(defmethod plan-select-item (query-plan (select-item list))
  (reduce #'plan-select-item (rest select-item)
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

(defmethod plan-clause (query-plan (clause list))
  (reduce #'plan-clause (rest clause)
	  :initial-value query-plan))

;;(defmethod compute-select-item ((select-item lisp) query-plan &rest args))
;; функция должна возвращать два значения - найденный узел и "остаток" пути, который не удалось найти
(defun get-plan-node (query-plan node &rest path)
  (let ((node (assoc node query-plan)))
    (when (not (null path))
      (apply #'get-plan-node node path))
    path)


  (reduce #'(lambda (nodes key)
	      (assoc key nodes))
	  (binding-path binding)
	  :initial-value query-plan))

;;  (let ((path (binding-path select-item)))
;;    (reduce #'(lambda (plan node)
;;		(find node (rest plan) :key #'first))
;;	    (rest path)
;;	    :initial-value (find (first path) query-plan
;;				 :key #'first))))

(defgeneric compute-select-item (select-item query-plan))

(defmethod compute-select-item ((select-item root-binding) query-plan)
  (list
   (class-mapping-of select-item)
   (assoc select-item query-plan)))

(defmethod compute-select-item ((select-item reference-binding) query-plan)
  (list
   (referenced-class-mapping-of
    (reference-mapping-of select-item))
   (get-plan-node select-item query-plan)))

(defmethod compute-select-item ((select-item value-binding) query-plan)
  (list
   (value-mapping-of select-item)
   (get-plan-node (parent-binding-of select-item) query-plan)))

(defgeneric compute-clause-item (query-plan select-item args))

(defmethod compute-clause-item (query-plan
				(select-item root-binding) args)
  (declare (ignore args))
  (list
   (class-mapping-of select-item)
   (assoc select-item query-plan)))

(defmethod compute-clause-item (query-plan
				(select-item reference-binding) args)
  (declare (ignore args))
  (list
   (referenced-class-mapping-of
    (reference-mapping-of select-item))
   (get-plan-node select-item query-plan)))

(defmethod compute-clause-item (query-plan
				(select-item value-binding) args)
  (declare (ignore args))
  (list
   (value-mapping-of select-item)
   (get-plan-node (parent-binding-of select-item) query-plan)))

(defmethod compute-clause-item (query-plan select-item args)
  (list* select-item
	 (mapcar #'(lambda (arg)
		     (apply #'compute-clause-item query-plan
			    arg))
		 args)))

(defun compute-clause (clause query-plan)
  (mapcar #'(lambda (clause-item)
	      (apply #'compute-clause-item
		     query-plan clause-item)) clause))

(defun make-query (select-list &key fetch single
		   where order-by having limit offset)
  (let ((join-plan
	 (reduce #'plan-clause
		 (append where order-by having)
		 :initial-value (reduce #'plan-select-item
					select-list
					:initial-value nil)))
	(fetch-plan
	 (reduce #'plan-clause fetch
		 :initial-value nil)))
    (list :select-list (mapcar #'(lambda (select-item)
				   (compute-select-item select-item join-plan))
			       select-list)
	  :from-clause join-plan
;;	  :where-clause (compute-clause where query-plan)
;;	  :order-by-clause (compute-clause order-by query-plan)
;;	  :having-clause (compute-clause having query-plan)
	  :fetch (compute-clause fetch fetch-plan)
;;				 (plan-clause nil fetch-also))
	  :limit limit
	  :offset offset)))
;;	  :single single)))

;;(defun db-read (class-name &key single fetch-also join
;;		where order-by having limit offset)
;;  (let ((root (get-class-mapping (find-class class-name))))
;;    (

;;пока предположим, что загрузка ассоциаций происходит только ленивым
;;способом

;;(defun join-fetch (binding reference))

;;(defgeneric load-row (loader row))