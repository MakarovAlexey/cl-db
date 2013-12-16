(in-package #:cl-db)

<<<<<<< HEAD
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
=======
(defclass query-info ()
  ((root-bindings :initarg :root-bindings
		  :initform (list)
		  :accessor root-bindings-of)
   (reference-bindings :initarg :reference-bindings
		       :initform (list)
		       :accessor reference-bindings-of)
   (value-bindings :initarg :value-bindings
		  :initform (list)
		  :accessor value-bindings-of)
   (expressions :initarg :expressions
		:initform (list)
		:accessor expressions-of)
   (fetched-references :accessor fetched-references-of)
   (order-by-clause :accessor order-by-clause-of)
   (having-clause :accessor having-clause-of)
   (where-clause :accessor where-clause-of)
   (select-list :accessor select-list-of)
   (limit :reader limit-of)
   (offset :reader offset-of)
   (single-instance :reader single-instance-p)))

(defclass value-node ()
  ((value-mapping :initarg :value-mapping
		  :reader value-mapping)))

(defclass object-node ()
  ((superclass-nodes :initarg :superclass-nodes
		     :reader superclass-nodes-of)
   (reference-nodes :initarg :reference-nodes
		    :reader reference-nodes-of)
   (value-nodes :initarg :value-nodes
	       :reader value-nodes-of)))

(defclass binding-node (object-node)
  ((subclass-nodes :initarg :subclass-nodes
		   :reader subclass-nodes-of)))

(defclass root-node (binding-node)
  ((root-binding :initarg :root-binding
		 :reader root-binding-of)))

(defclass reference-node (binding-node)
  ((reference-binding :initarg :reference-binding
		      :reader reference-binding-of)))

(defclass inheritance-node ()
  ((inheritance-mapping :initarg :inheritance-mapping
			:reader inheritance-mapping-of)))

(defclass superclass-node (inheritance-node)
  ((superclass-nodes :initarg :superclass-nodes
		     :reader superclass-nodes-of)))

(defclass binding-superclass-node (superclass-node object-node)
  ())

(defclass subclass-node (superclass-node)
  ((subclass-nodes :initarg :subclass-nodes
		   :reader subclass-nodes-of)))

;; Query AST

(defclass fetch-query ()
  ((select-list :initarg :select-list
		:reader select-list-of)
   (from-clause :initarg :from
		:reader from-clause-of)))

(defclass sql-query (fetch-query)
  ((where-clause :initarg :where
>>>>>>> 4bbfe3b5e335d899f4112207f3b24390329ab646
		 :reader where-clause-of)
   (order-by-clause :initform (list)
		    :initarg :order-by-clause
		    :reader order-by-clause-of)
   (having-clause :initform (list)
		  :initarg :having-cluse
		  :reader having-clause-of)))


<<<<<<< HEAD

;;(defclass
;;   (fetched-references :initarg :fetched-references
;;		       :reader fetched-references-of)))

(defun find-node (query-plan &rest clauses)
  (reduce #'(lambda (query-plan clause)
	      (when (not (null query-plan))
		(find clause (children-of query-plan)
		      :key #'clause-of)))
	  clauses :initial-value query-plan))
=======
(defun make-root-node (root-binding query-info)
  (let ((class-mapping (class-mapping-of root-binding)))
    (make-instance 'root-node
		   :root-binding root-binding
		   :reference-nodes
		   (mapcar #'(lambda (reference-binding)
			       (make-reference-node reference-binding
						    query-info))
			   (find-reference-bindings root-binding
						    query-info))
		   :value-nodes
		   (mapcar #'make-value-node
			   (find-value-bindings root-binding query-info))
		   :superclass-nodes
		   (compute-superclass-nodes root-binding class-mapping
					     query-info)
		   :subclass-nodes
		   (when (member root-binding (select-list-of query-info))
		     (compute-subclass-nodes class-mapping)))))

(defun find-node ())

(defgeneric compute-select-item (select-item query-trees))

(defmethod compute-select-item ((select-item root-binding) query-trees)
  (let ((root-node
	 (find select-item query-trees :key #'root-binding-of)))
    

(defmethod compute-select-item ((select-item reference-binding) query-trees)
  

(defmethod compute-select-item ((select-item value-binding) query-trees)
  (

;; query-tree -> loaders -> sql-query
(defun compute-sql-select-list (select-list query-trees)
  (loop for select-item in select-list
     append (compute-select-item select-item query-trees)))
>>>>>>> 4bbfe3b5e335d899f4112207f3b24390329ab646

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
