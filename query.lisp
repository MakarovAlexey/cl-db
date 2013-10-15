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
		 :reader where-clause-of)
   (order-by-clause :initarg :order-by
		    :reader order-by-clause-of)
   (having-clause :initarg :having
		  :reader having-clause-of)
   (limit :initarg :limit
	  :reader limit-of)
   (offset :initarg :offset
	   :reader offset-of)))

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
		   expressions value-bindings)
  (make-instance 'query-info
		 :value-bindings
		 (append (value-bindings-of query-info) value-bindings)
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
			  :value-bindings (value-bindings-of query-info-2)))
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
	 (accumulate visitor :value-bindings (list instance))))

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

(defun find-reference-bindings (parent-binding query-info)
  (remove-if-not #'(lambda (binding)
		     (and (eq (parent-binding-of binding)
			      parent-binding)
			  (null (path-of
				 (reference-mapping-of binding)))))
		 (reference-bindings-of query-info)))

(defun find-value-bindings (parent-binding query-info)
  (remove-if-not #'(lambda (binding)
		     (and (eq (parent-binding-of binding)
			      parent-binding)
			  (null (path-of
				 (value-mapping-of binding)))))
		 (value-bindings-of query-info)))

(defun compute-value-superclass-node (value-binding
				      inheritance-mapping &rest path)
  (make-instance 'binding-superclass-node
		 :inheritance-mapping
		 inheritance-mapping
		 :value-bindings
		 (when (null path)
		   (list (make-value-node value-binding)))
		 :superclass-nodes
		 (when (not (null path))
		   (apply #'compute-value-superclass-node
			  value-binding path))))

(defun compute-value-superclass-nodes (query-info binding)
  (mapcar #'(lambda (value-binding)
	      (apply #'compute-value-superclass-node
		     value-binding (path-of value-binding)))
	  (find-value-bindings binding query-info)))

(defun compute-reference-superclass-node (reference-binding query-info
					  inheritance-mapping &rest path)
  (make-instance 'binding-superclass-node
		 :inheritance-mapping
		 inheritance-mapping 
		 :reference-bindings
		 (when (null path)
		   (list (make-reference-node reference-binding
					      query-info)))
		 :superclass-nodes
		 (when (not (null path))
		   (apply #'compute-reference-superclass-node
			  reference-binding query-info path))))

(defun compute-reference-superclass-nodes (query-info bindng)
  (mapcar #'(lambda (reference-binding)
	      (apply #'compute-reference-superclass-node
		     reference-binding query-info
		     (path-of reference-binding)))
	  (find-reference-bindings bindng query-info)))

(defun compute-all-superclass-nodes (class-mapping)
  (mapcar #'(lambda (inheritance-mapping)
	      (make-instance 'binding-superclass-node
			     :inheritance-mapping
			     inheritance-mapping
			     :superclass-nodes
			     (compute-all-superclass-nodes
			      (superclass-mapping-of inheritance-mapping))))
	  (superclasses-inheritance-mappings-of class-mapping)))

(defun compute-superclass-nodes (binding class-mapping query-info)
  (append
   (compute-value-superclass-nodes query-info binding)
   (compute-reference-superclass-nodes query-info binding)
   (when (member binding (select-list-of query-info))
     (compute-all-superclass-nodes class-mapping))))

(defun compute-subclass-superclass-nodes (class-mapping)
  (mapcar #'(lambda (inheritance-mapping)
	      (make-instance 'superclass-node
			     :inheritance-mapping inheritance-mapping
			     :superclass-nodes
			     (compute-subclass-nodes
			      (superclass-mapping-of inheritance-mapping))))
	  (superclasses-inheritance-mappings-of class-mapping)))

(defun compute-subclass-nodes (class-mapping)
  (mapcar #'(lambda (inheritance-mapping)
	      (make-instance 'subclass-node
			     :inheritance-mapping
			     inheritance-mapping
			     :superclass-nodes
			     (compute-subclass-superclass-nodes
			      (superclass-mapping-of inheritance-mapping))
			     :subclass-nodes
			     (compute-subclass-nodes
			      (subclass-mapping-of inheritance-mapping))))
	  (subclasses-inheritance-mappings-of class-mapping)))

(defun make-value-node (&rest value-bindings)
  (make-instance 'value-node :value-bindings value-bindings))

(defun make-reference-node (reference-binding query-info)
  (let ((class-mapping
	 (class-mapping-of (reference-mapping-of reference-binding))))
    (make-instance 'reference-node
		   :reference-bindings (list reference-binding)
		   :reference-nodes
		   (mapcar #'(lambda (reference-binding)
			       (make-reference-node reference-binding
						    query-info))
			   (find-reference-bindings reference-binding
						    query-info))
		   :value-nodes
		   (mapcar #'make-value-node
			   (find-value-bindings reference-binding
						query-info))
		   :superclass-nodes
		   (compute-superclass-nodes reference-binding
					     class-mapping query-info)
		   :subclass-nodes
		   (when (member reference-binding
				 (select-list-of query-info))
		     (compute-subclass-nodes
		      (class-mapping-of reference-binding))))))

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
		 


;; query-tree -> loaders -> sql-query
;;???(defun compute-sql-select-list (select-list query-trees)
;;  (reduce #'(lambda (sql-select-list object-select-item)
;;	      (list* (compute-sql-select-item object-select-item)
;;		     sql-select-list))
;;	  select-list))

;;(defun make-sql-query (query-trees query-info)
;;  (make-instance 'sql-query
;;		 :select-list
;;		 (compute-select-items query-tree select-list)
;;		 :from-clause
;;		 (compute-from-clause query-tree)
;;		 :where-clause
;;		 (compute-where-clause query-tree where)
;;		 :group-by-clause
;;		 (compute-group-by-clause query-tree query-info)
;;		 :having-clause
;;		 (compute-having-clause query-tree having)
;;		 :order-by-clause
;;		 (compute-order-by-clause query-tree order-by)
;;		 :limit limit :offset offset))

;;(defun append-fetch (sql-query fetch-reference-bindings)
;;  (if (not (null (or (limit-of sql-query)
;;		     (offset-of sql-query))))
      ;;wrap query as subquery
      ;;make query with additional joins
;;      (make-instance 'sql-query

;;  (let ((
;;  (format t (concatenate 'string
;;			 "~@[SELECT ~<~@{~w~^, ~:_~}~:>~]"
;;			 "~@[~%  FROM ~<~@{~w~^, ~:_~}~:>~]"
;;			 "~@[~% ORDER BY ~<~@{~w~^, ~:_~}~:>~]"
;;			 "~@[~%HAVING ~<~@{~w~^, ~:_~}~:>~]"
;;			 "~@[~% GROUP BY ~<~@{~w~^, ~:_~}~:>~]"
;;			 (compute-sql-select-list
;;			  (select-list-of query-info) query-trees)
;;			 (make-list 100)
;;			 (make-list 20)
;;			 (make-list 10)
;;			 (make-list 10))))

(defun make-query (select-list &key where order-by having
		   fetch-also limit offset single)
  (let* ((query-info
	  (make-query-info select-list where order-by having
			   fetch-also limit offset single))
	 (query-tree
	  (mapcar #'(lambda (root-binding)
		      (make-root-node root-binding query-info))
		  (root-bindings-of query-info))))
    query-tree))
;;	 (sql-query
;;	  (append-fetch (make-sql-query query-info query-tree)
;;			fetch-also)))
 ;;   (make-instance 'db-query
;;		   :sql-query sql-query
;;		   :parameters (compute-parameters query-info)
;;		   :query-loader (compute-query-loader sql-query
;;						       single
;;						       query-tree))))
