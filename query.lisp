(in-package #:cl-db)

(defclass query-plan ()
  ((children :initarg :children
	     :reader children-of)))

(defclass plan-node (query-plan)
  ((clause :initarg :clause
	   :reader clause-of)))

(defclass query ()
  ((plan-nodes :initform (list)
	       :initarg :plan-nodes
	       :reader plan-nodes-of)
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
		  :reader having-clause-of)))



;;(defclass
;;   (fetched-references :initarg :fetched-references
;;		       :reader fetched-references-of)))

(defun find-node (query-plan &rest clauses)
  (reduce #'(lambda (query-plan clause)
	      (when (not (null query-plan))
		(find clause (children-of query-plan)
		      :key #'clause-of)))
	  clauses :initial-value query-plan))

(defgeneric plan-select-item (query-plan binding))

(defmethod plan-select-item (query-plan (binding root-binding))
  (when (not (find binding (children-of query-plan) :key #'clause-of))
    (make-instance 'query-plan
		   :children (list*
			      (make-instance 'plan-node :clause binding)
			      (children-of query-plan)))))

(defmethod plan-select-item (query-plan (binding reference-binding))
  (

(defun make-query (select-list &key where order-by having
		   fetch-also limit offset single)
  (reduce #'plan-fetch-also-clause fetch-also
	  :initial-value
	  (reduce #'plan-clause (append where order-by having)
		  :initial-value
		  (reduce #'plan-select-item
			  :initial-value (make-instance 'query)))))