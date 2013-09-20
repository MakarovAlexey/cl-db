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

(defclass expression ()
  ((expression-type :initarg :expression-type
		    :reader expression-type-of)
   (arguments :initarg :arguments
	      :reader arguments-of)))

(defclass query-info ()
  ((root-bindings :initarg :root-bindings
		  :initform (list)
		  :accessor root-bindings-of)
   (reference-bindings :initarg :reference-bindings
		       :initform (list)
		       :accessor reference-bindings-of)
   (values-access :initarg :values-access
		  :initform (list)
		  :accessor values-access-of)
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

(defclass query-node ()
  ((inheritance-nodes :initform (make-hash-table)
		      :reader inheritance-nodes-of)
   (reference-nodes :initform (list)
		    :accessor reference-nodes-of)
   (values-access :initform (list)
		  :accessor values-access-of)))

(defclass binding-node (query-node)
  ((query-binding :initarg :query-binding
		  :reader query-binding-of)
   (extension-nodes :initform (list)
		    :accessor extension-nodes-of)))

(defclass binding-inheritance-node (query-node)
  ((inheritance-mapping :initarg :inheritance-mapping
			:reader inheritance-mapping-of)))

(defclass extension-inheritance-node ()
  ((inheritance-mapping :initarg :inheritance-mapping
			:reader inheritance-mapping-of)
   (extension-inheritance-nodes :initform (make-hash-table)
				:reader extension-inheritance-nodes-of)))

(defclass extension-node (extension-inheritance-node)
  ((extension-nodes :initform (list)
		    :accessor extension-nodes-of)))

(defclass db-query ()
  ((sql-string :initarg :sql-string :reader sql-string-of)))

;;(defclass select-item ())

;;(defclass where-clause ())

;;(defclass order-by-clause ())

;;(defclass having-clause ())

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

(defun accumulate (query-info &key roots references
		   expressions values-access)
  (make-instance 'query-info
		 :values-access
		 (append (values-access-of query-info) values-access)
		 :expressions
		 (append (expressions-of query-info) expressions)
		 :reference-bindings
		 (append (reference-bindings-of query-info) references)
		 :root-bindings
		 (append (root-bindings-of query-info) roots)))

(defun append-accumulated (query-info &rest query-infos)
  (reduce #'(lambda (query-info query-info-2)
	      (accumulate query-info 
			  :roots (root-bindings-of query-info-2)
			  :references (reference-bindings-of query-info-2)
			  :expressions (expressions-of query-info-2)
			  :values-access (values-access-of query-info-2)))
	  query-infos :initial-value query-info))

(defgeneric visit (instance &optional visitor))

(defmethod visit ((instance root-binding)
		  &optional (visitor (make-instance 'query-info)))
  (accumulate visitor :roots (list instance)))

(defmethod visit ((instance reference-binding)
		  &optional (visitor (make-instance 'query-info)))
  (visit (parent-binding-of instance)
	 (accumulate visitor :references (list instance))))

(defmethod visit ((instance value-binding)
		  &optional (visitor (make-instance 'query-info)))
  (visit (parent-binding-of instance)
	 (accumulate visitor :values-access (list instance))))

(defmethod visit ((instance expression)
		  &optional (visitor (make-instance 'query-info)))
  (apply #'append-accumulated
	 (accumulate visitor :expressions (list instance))
	 (mapcar #'visit (arguments-of instance))))

(defun make-query-info (select-list where-clause order-by-clause
			having-clause fetched-references
			limit offset single-instance)
  (let ((query-info (apply #'append-accumulated
			   (append
			    (mapcar #'visit select-list)
			    (mapcar #'visit where-clause)
			    (mapcar #'visit order-by-clause)
			    (mapcar #'visit having-clause)
			    (mapcar #'visit fetched-references)))))
    (with-slots ((select-list-slot select-list)
		 (where-clause-slot where-clause)
		 (order-by-clause-slot order-by-clause)
		 (having-clause-slot having-clause)
		 (fetched-references-slot fetched-references)
		 (limit-slot limit)
		 (offset-slot offset)
		 (single-instance-slot single-instance))
	query-info
      (setf fetched-references-slot fetched-references
	    order-by-clause-slot order-by-clause
	    having-clause-slot having-clause
	    where-clause-slot where-clause
	    select-list-slot select-list
	    single-instance-slot single-instance
	    offset-slot offset
	    limit-slot limit))
    query-info))

(defun get-reference-bindings (query-binding query-info)
  (remove query-binding
	  (reference-bindings-of query-info)
	  :key #'parent-binding-of
	  :test-not #'eq))

(defun get-values-access (query-binding query-info)
  (remove query-binding
	  (values-access-of query-info)
	  :key #'parent-binding-of
	  :test-not #'eq))

(defun ensure-inheritance-node (query-node inheritance-mapping)
  (let ((inheritance-nodes (inheritance-nodes-of query-node)))
    (multiple-value-bind (inheritance-node presentp)
	(gethash inheritance-mapping inheritance-nodes)
      (if (not presentp)
	  (setf (gethash inheritance-mapping inheritance-nodes)
		(make-instance 'binding-inheritance-node
			       :inheritance-mapping
			       inheritance-mapping))
	  inheritance-node))))

(defun ensure-path-node (query-node inheritance-mapping &rest path)
  (let ((inheritance-node
	 (ensure-inheritance-node query-node inheritance-mapping)))
    (if (not (null path))
	(apply #'ensure-path-node inheritance-node path)
	inheritance-node)))

(defun add-value-binding (query-node value-binding)
  (push value-binding
	(values-access-of
	 (apply #'ensure-inheritance-node query-node
		(path-of (value-mapping-of value-binding))))))

(defun add-reference-node (query-node reference-binding query-info)
  (let ((reference-mapping (reference-mapping-of reference-binding)))
    (push
     (make-instance 'binding-node
		    :class-mapping (class-mapping-of reference-mapping)
		    :query-binding reference-binding
		    :query-info query-info)
     (reference-nodes-of
      (apply #'ensure-path-node query-node
	     (path-of reference-mapping))))))

(defun compute-inheritance-nodes (query-node class-mapping)
  (dolist (inheritance-mapping
	    (superclasses-inheritance-mappings-of class-mapping))
    (compute-inheritance-nodes
     (ensure-inheritance-node query-node inheritance-mapping)
     (superclass-mapping-of inheritance-mapping))))

(defun compute-extension-nodes (query-node class-mapping)
  (dolist (inheritance-mapping
	    (subclasses-inheritance-mappings-of class-mapping))
    (compute-extension-nodes
     (push (make-instance 'extension-node
			  :inheritance-mapping inheritance-mapping)
	   (extension-nodes-of query-node))
     (subclass-mapping-of inheritance-mapping))))

(defmethod initialize-instance :after ((instance binding-node)
				       &key query-binding query-info
				       class-mapping)
  (dolist (value-binding (get-values-access query-binding query-info))
    (add-value-binding instance value-binding))
  (dolist (reference-binding (get-reference-bindings query-binding query-info))
    (add-reference-node instance reference-binding query-info))
  (when (member query-binding (select-list-of query-info))
    (compute-inheritance-nodes instance class-mapping)
    (compute-extension-nodes instance class-mapping)))

(defun make-root-node (root-binding query-info)
  (make-instance 'binding-node
		 :class-mapping (class-mapping-of root-binding)
		 :query-binding root-binding
		 :query-info query-info))


(defun make-sql-query (query-trees query-info)
  
)  

(defun make-query (select-list &key where order-by having
		   fetch-also limit offset single)
  (let* ((query-info
	  (make-query-info select-list where order-by having
			   fetch-also limit offset single))
	 (query-tree
	  (mapcar #'(lambda (root-binding)
		      (make-root-node root-binding query-info))
		  (root-bindings-of query-info))))
    (make-instance 'db-query
		   :sql-string (make-sql-query query-tree query-info)
;;		   :parameters (make-parameters select-list where
;;						order-by having
;;						limit offset)
;;		   :loader (make-loader table-tree select-list
;;					sinlge))))
