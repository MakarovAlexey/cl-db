(in-package #:cl-db)

(defvar *table-index*)

(defvar *inheritance-nodes*)

(defun make-alias (&rest name-parts)
  (format nil "~{~(~a~)_~}~a" name-parts (incf *table-index*)))

(defun call-with-inheritance-nodes (inheritance-nodes thunk)
  (let ((*inheritance-nodes* (if (not (null inheritance-nodes))
				 inheritance-nodes
				 (make-hash-table))))
    (funcall thunk)))

(defmacro with-inheritance-nodes ((&optional inheritance-nodes) &body body)
  `(call-with-inheritance-nodes ,inheritance-nodes
				#'(lambda () ,@body)))
  
(defun add-class-node (class-node &optional (inheritance-nodes *inheritance-nodes*))
  (let ((class-name
	 (class-name-of
	  (class-mapping-of class-node))))
    (multiple-value-bind (class-node presentp)
	(gethash class-name inheritance-nodes)
      (when presentp
	(error "node for class ~a already added" class-name))
      (setf (gethash class-name inheritance-nodes) class-node))))

;; TODO, path сделать динамической переменной и использовать при инициализации свойств и ссылок
(defun ensure-superclass-node (superclass-mapping path
			       &optional (inheritance-nodes *inheritance-nodes*))
  (multiple-value-bind (class-node presentp)
      (gethash (reference-class-of superclass-mapping) inheritance-nodes)
    (if (not presentp)
	(make-instance 'superclass-node
		       :alias (make-alias)
		       :class-mapping (get-class-mapping
				       (reference-class-of superclass-mapping))
		       :foreign-key (foreign-key-of superclass-mapping)
		       :path path)
	class-node)))

(defun plan-direct-references (class-node)
  (let ((class-mapping
	 (class-mapping-of class-node)))
    (reduce #'(lambda (reference result)
		(acons reference class-node result))
	    (append
	     (many-to-one-mappings-of class-mapping)
	     (one-to-many-mappings-of class-mapping))
	    :from-end t
	    :initial-value nil)))

(defmethod initialize-instance :after ((instance class-node)
				       &key class-mapping path
					 (superclass-mappings
					  (superclass-mappings-of class-mapping)))
  (add-class-node instance)
  (with-slots (superclass-nodes direct-references)
      instance
    (let ((path (list* instance path)))
      (setf superclass-nodes
	    (mapcar #'(lambda (superclass-mapping)
			(ensure-superclass-node superclass-mapping path))
		    superclass-mappings)))
    (setf direct-references (plan-direct-references instance))))

(defmethod initialize-instance :after ((instance concrete-class-node)
				       &key &allow-other-keys)
  (with-slots (inheritance-nodes)
      instance
    (setf inheritance-nodes *inheritance-nodes*)))

(defun plan-effective-references (class-node)
  (reduce #'append (superclass-nodes-of class-node)
	  :key #'plan-effective-references
	  :from-end t
	  :initial-value (direct-references-of class-node)))

(defun plan-superclasses-properties (class-node)
  (reduce #'append
	  (superclass-nodes-of class-node)
	  :key #'plan-properties
	  :from-end t
	  :initial-value nil))

(defun plan-properties (class-node)
  (reduce #'list*
	  (property-mappings-of
	   (class-mapping-of class-node))
	  :key #'(lambda (property-mapping)
		   (make-instance 'property-node
				  :class-node class-node
				  :mapping property-mapping))
	  :from-end t
	  :initial-value (plan-superclasses-properties class-node)))

(defmethod initialize-instance :after ((instance root-node)
				       &key &allow-other-keys)
  (with-slots (properties effective-references)
      instance
    (setf properties (plan-properties instance))
    (setf effective-references (plan-effective-references instance))))

(defun find-slot-name (class reader-name)
  (or
   (find-if #'(lambda (slot-definition)
		(find reader-name
		      (slot-definition-readers slot-definition)))
	    (class-direct-slots class))
   (find-slot-name (find-if #'(lambda (class)
				(find-slot-name class reader-name))
			    (class-direct-superclasses class))
		   reader-name)))

(defun get-slot-name (class reader)
  (let ((slot-definition
	 (find-slot-name class (generic-function-name reader))))
    (when (null slot-definition)
      (error "Slot with reader ~a not found for class ~a"
	     reader (class-name class)))
    (slot-definition-name slot-definition)))

;;; Joins

(defun join (root-class-node reader &key alias join)
  (destructuring-bind (reference . class-node)
      (assoc (get-slot-name
	      (find-class
	       (class-name-of
		(class-mapping-of root-class-node))) reader)
	     (effective-references-of root-class-node)
	     :key #'slot-name-of)
    (with-inheritance-nodes ()
      (let* ((reference-node
	      (make-instance 'reference-node
			     :class-mapping (get-class-mapping
					     (reference-class-of reference))
			     :class-node class-node
			     :reference reference))
	     (args
	      (when (not (null join))
		(multiple-value-call #'append
		  (funcall join reference-node)))))
	(if (not (null alias))
	    (list* alias reference-node args)
	    args)))))

(defun disjunction (restriction &rest more-restrictions)
  (make-instance 'disjunction
		 :args (list* restriction more-restrictions)))

(defun conjunction (restriction &rest more-restrictions)
  (make-instance 'conjunction
		 :args (list* restriction more-restrictions)))

(defun restrict (property &key equal not-equal not is like not-like
			    less-than less-than-or-equal
			    more-than more-than-or-equal)
  (let ((operations
	 (list (cons 'equality equal)
	       (cons 'not-equal not-equal)
	       (cons 'not not)
	       (cons 'is is)
	       (cons 'like like)
	       (cons 'not-like not-like)
	       (cons 'less-than less-than)
	       (cons 'less-than-or-equal less-than-or-equal)
	       (cons 'more-than more-than)
	       (cons 'more-than-or-equal more-than-or-equal))))
  (make-instance 'conjunction :args 
		 (loop for (class-name . parameter) in operations
		    when (not (null parameter))
		    collect (make-instance class-name
					   :lhs property
					   :rhs parameter)))))

(defgeneric projection (descriptor &rest args))

(let ((projections
       (list #'+ 'addition
	     #'- 'subtracion
	     #'* 'multiplication
	     #'/ 'division
	     #'member 'memeber)))
  (defmethod projection ((descriptor function) &rest args)
    (let ((projection-name (getf projections descriptor)))
      (apply #'projection projection-name args))))

(defmethod projection ((descriptor symbol) &rest args)
  (make-instance descriptor :arguments args))

(defmethod projection ((descriptor string) &rest args)
  (make-instance 'rdbms-function :name descriptor :args args))

(defgeneric aggregation (descriptor &rest args))

(let ((aggregations
       (list #'+ 'sql-sum
	     #'min 'sql-min
	     #'max 'sql-max
	     #'every 'sql-every
	     #'count 'sql-count)))
  (defmethod aggregation ((descriptor function) &rest args)
    (let ((class-name (getf aggregations descriptor)))
      (apply #'aggregation class-name args))))

(defmethod aggregation ((descriptor symbol) &rest args)
  (make-instance descriptor :arguments args))

(defmethod aggregation ((descriptor string) &rest args)
  (make-instance 'rdbms-aggregation :name descriptor :args args))

(defvar *common-table-expression*)

(defmethod references-of ((node recursive-class-node))
  (mapcar #'(lambda (reference)
	      (cons (first reference) node))
	  (references-of
	   (class-node-of node))))

(defmethod class-mapping-of ((class-node recursive-class-node))
  (class-mapping-of (class-node-of class-node)))

;; redefine property search for recursive nodes
(defmethod properties-of ((class-node recursive-class-node))
  (mapcar #'(lambda (property)
	      (cons (car property) class-node))
	  (properties-of (class-node-of class-node))))

(defun recursive (class-node) ;; CTE name ?
  (make-instance 'recursive-class-node :class-node class-node))

(defun ascending (arg)
  (make-instance 'ascending :arg arg))

(defun descending (arg)
  (make-instance 'descending :arg arg))

;; Select list

;;; Property

(defgeneric property (node reader))

(defmethod property ((root-node root-node) reader)
  (let* ((class-name
	  (class-name-of (class-mapping-of root-node)))
	 (slot-name
	  (get-slot-name (find-class class-name) reader)))
    (or
     (find slot-name
	   (properties-of root-node)
	   :key #'(lambda (property-node)
		    (slot-name-of
		     (mapping-of property-node))))
     (error "Property mapping for slot-name ~a of class mapping ~a not found"
	    slot-name class-name))))

(defmethod property ((recursive-node recursive-class-node) reader)
  (let ((property-node
	 (property (class-node-of recursive-node) reader)))
    (cons (first property-node) recursive-node)))

(defmethod property ((class-selection root-class-selection) reader)
  (property (concrete-class-node-of class-selection) reader))

(defun select-subclass (subclass-mapping &optional path)
  (make-instance 'subclass-selection :concrete-class-node
		 (make-instance 'concrete-class-node :path path
				:class-mapping
				(get-class-mapping
				 (reference-class-of subclass-mapping)))))

(defmethod initialize-instance :after ((instance root-class-selection)
				       &key concrete-class-node)
  (with-slots (subclass-selections) ;; references) ;;(columns 
      instance
;;    (setf expressions ;; columns
;;	  (reduce #'(lambda (column-name result)
;;		      (acons column-name
;;			     (make-alias column-name)
;;			     result))
;;		  (columns-of
;;		   (class-mapping-of root-node))
;;		  :initial-value nil))
    ;; Добавить присвоение inheritance-nodes
    (with-inheritance-nodes ((inheritance-nodes-of concrete-class-node))
      (setf subclass-selections
	    (mapcar #'(lambda (subclass-mapping)
			(select-subclass subclass-mapping))
		    (subclass-mappings-of
		     (class-mapping-of concrete-class-node)))))))

(defmethod initialize-instance :after ((instance subclass-selection)
				       &key concrete-class-node)
  (with-slots (subclass-selections) ;; references) ;;(columns 
      instance
    (setf subclass-selections
	    (mapcar #'(lambda (subclass-mapping)
			(select-subclass subclass-mapping))
		    (subclass-mappings-of
		     (class-mapping-of concrete-class-node))))))

;;    (setf recursive
;;	  (when (not (null recursive))
;;	    (restrict
;;	     (recursive (root-node-of instance)) :equal recursive)))

(defmethod initialize-instance :after ((instance reference-fetching)
				       &key fetch)
  (with-slots (fetched-references)
      instance
    (setf fetched-references
	  (when (not (null fetch))
	    (multiple-value-list
	     (funcall fetch instance))))))

;;(defmethod initialize-instance :after ((instance subclass-node)
;;				       &key class-mapping)
;;  (setf (slot-value instance 'subclass-nodes)
;;	(mapcar #'(lambda (subclass-mapping)
;;		    (make-subclass-node subclass-mapping instance))
;;		(subclass-mappings-of class-mapping))))

;; сосопставление выражений и колонок по псевдонимам происходит при
;; планировании запроса

(defun fetch (class-selection reader &key class-name fetch recursive)
  (let* ((class-node (concrete-class-node-of class-selection))
	 (reference
	  (assoc (get-slot-name
		  (find-class
		   (if (not (null class-name))
		       class-name
		       (class-name-of
			(class-mapping-of class-node)))) reader)
		 (if (not (null class-name))
		     (direct-references-of
		      (gethash class-name
			       (inheritance-nodes-of class-node)))
		     (effective-references-of class-node))
		 :key #'slot-name-of)))
    (with-inheritance-nodes ()
      (make-instance 'reference-fetching
		     :concrete-class-node
		     (make-instance 'concrete-class-node
				    :class-mapping (get-class-mapping
						    (reference-class-of
						     (first reference))))
		     :class-selection class-selection
		     :reference reference
		     :recursive-selection recursive
		     :fetch fetch))))

(defmethod join-list-of ((cte common-table-expression))
  (join-list-of (query-of cte)))

(defmethod roots-of ((cte common-table-expression))
  (roots-of (query-of cte)))

(defun make-root (class-name)
  (with-inheritance-nodes ()
    (make-instance 'root-node :class-mapping (get-class-mapping class-name))))

(defgeneric compute-joining-from-clause (expression))

(defmethod compute-joining-from-clause ((expression expression))
  (mapcar #'compute-joining-from-clause
	  (arguments-of expression)))

(defmethod compute-joining-from-clause ((expression property-node))
  (let ((path (path-of expression)))
    (reduce #'(lambda (result node)
		(list node result))
	    (rest path)
	    :from-end t
	    :initial-value (list (first path)))))

(defun compute-concrete-class-from-clause (concrete-class-node)
  (list* concrete-class-node
	 (mapcar #'compute-joining-from-clause
		 (superclass-nodes-of concrete-class-node))))

(defmethod compute-joining-from-clause ((expression concrete-class-node))
  (compute-concrete-class-from-clause expression))

(defun root (tree)
  (first tree))

(defun children (tree)
  (rest tree))

(defun merge-join-tree (from-clause tree-root &rest children)
  (let ((tree
	 (find tree-root from-clause :key #'root)))
    (list* tree-root
	   (if (not (null tree))
	       (merge-from-clause (children tree) children)
	       children))))

(defun merge-from-clause (from-clause-1 from-clause-2)
  (reduce #'(lambda (tree result)
	      (apply #'merge-join-tree result tree))
	  from-clause-2
	  :initial-value from-clause-1))

(defun table-alias (class-node table-aliases)
  (rest (assoc class-node table-aliases)))

(defun compute-table-aliases (from-clause)
  (reduce #'append from-clause
	  :key (lambda (tree)
		 (let ((class-node (root tree)))
		   (acons class-node
			  (make-alias
			   (class-name-of
			    (class-mapping-of class-node)))
			  (compute-table-aliases
			   (children tree)))))))

(defun plan-column (column-name class-node)
  (make-instance 'column-plan
		 :column-name column-name
		 :class-node class-node))

(defun plan-node-columns (class-node)
  (reduce #'append
	  (superclass-nodes-of class-node)
	  :key #'plan-node-columns
	  :initial-value (mapcar #'(lambda (column-name)
				     (plan-column column-name class-node))
				 (columns-of
				  (class-mapping-of class-node)))))

(defgeneric get-columns (expression))

(defmethod get-columns ((expression expression))
  (reduce #'append (arguments-of expression) :key #'plan-columns))

(defun get-column (column-name class-node)
  (list class-node column-name))
;;  (make-instance 'column-expression
;;		 :class-node class-node
;;		 :alias (make-alias column-name)
;;		 :table-alias (rest (assoc class-node table-aliases))
;;		 :column-name column-name))

(defmethod get-columns ((expression property-node))
  (list
   (plan-column (column-of
		 (mapping-of expression))
		(class-node-of expression))))

(defmethod get-columns ((expression class-node))
  (reduce #'append (superclass-nodes-of expression)
	  :key #'get-columns
	  :initial-value
	  (mapcar #'(lambda (column-name)
		      (plan-column column-name expression))
		  (columns-of
		   (mapping-of expression)))))

(defun make-common-table-expression (name columns from-clause
				     table-aliases aux-clause
				     recursive-clause)
  (make-instance 'common-table-expression
		 :name name
		 :select-list columns
		 :from-clause from-clause
		 :table-aliases table-aliases
		 :aux-clause aux-clause
		 :recursive-clause recursive-clause))

(defun column-name (node-column)
  (second node-column))

(defun select-columns (column-plans table-aliases)
  (reduce #'append
	  column-plans
	  :key (lambda (column-plan)
		 (destructuring-bind ((class-node column-name) . alias)
		     column-plan
		   (list :select-item
			 :column column-name
			 (gethash class-node table-aliases)
			 :as alias)))
	  :from-end t))

(defun join-superclass (tree subclass-table-alias table-aliases)
  (let* ((superclass-node (root tree))
	(superclass-mapping
	 (class-mapping-of superclass-node))
	(superclass-table-alias
	 (table-alias superclass-node table-aliases)))
    (list*
     (list* :left-join (table-name-of superclass-mapping)
	    :as (table-alias superclass-node table-aliases)
	    :on (reduce #'append
			(mapcar #'(lambda (pk-column fk-column)
				    (let ((lhs
					   (cons pk-column superclass-table-alias))
					  (rhs
					   (cons fk-column subclass-table-alias)))
				      (list lhs := rhs)))
				(foreign-key-of superclass-node)
				(primary-key-of superclass-mapping))))
     (join-superclasses (children tree)
			superclass-table-alias
			table-aliases))))

(defun join-superclasses (children alias table-aliases)
  (reduce #'append children
	  :key #'(lambda (tree)
		   (join-superclass tree alias table-aliases))))

(defun join-root (tree table-aliases)
  (let* ((root-node (root tree))
	 (alias (table-alias root-node table-aliases))
	 (children (children tree)))
    (list*
     (table-name-of (class-mapping-of root-node)) :as alias
     (join-superclasses children alias table-aliases))))

(defun make-from-clause (from-clause table-aliases)
  (reduce #'append from-clause
	  :key #'(lambda (tree)
		   (join-root tree table-aliases))))

(defun compute-recursive-joining (expressions from-clause
				  table-aliases aux-clause recursive)
  (let ((name "joining")
	(column-plans
	 (reduce #'(lambda (result node-column)
		     (acons node-column
			    (make-alias
			     (column-name node-column))
			    result))
		 (remove-duplicates
		  (get-columns expressions) :test #'equal) ;; list of (list class-node oplumn-name)
		 :from-end t
		 :initial-value nil)))
    (make-instance 'recursive-joining
		   :name name
		   :columns column-plans
		   :from-clause from-clause
		   :common-table-expression
		   (make-common-table-expression name
						 (select-columns column-plans table-aliases)
						 (make-from-clause from-clause table-aliases)
						 table-aliases
						 aux-clause
						 recursive))))

(defun compute-joining (from-clause table-aliases)
  (make-instance 'joining
		 :from-clause from-clause
		 :table-aliases table-aliases))

(defun compute-join-list (expressions aux-clause recursive-clause)
  (let* ((from-clause
	  (reduce #'merge-from-clause expressions
		  :key #'compute-joining-from-clause
		  :initial-value nil))
	 (table-aliases
	  (compute-table-aliases from-clause)))
    (if (not (null recursive-clause))
	(compute-recursive-joining expressions from-clause
				   table-aliases aux-clause
				   recursive-clause)
	(compute-joining from-clause table-aliases))))

(defgeneric select-item (expression))

(defmethod select-item ((expression property-node))
  expression)

(defmethod select-item ((expression expression))
  expression)

(defmethod select-item ((expression root-node))
  (make-instance 'root-class-selection
		 :concrete-class-node expression))

(defun compute-selection (roots join aux recursive select where having
			  order-by limit offset)
  (let* ((selectors (if (listp roots)
			(mapcar #'(lambda (root)
				    (make-root root))
				roots)
			(list (make-root roots))))
	 (join-list (multiple-value-call #'append selectors
					 (when (not (null join))
					   (apply join selectors))))
	 (aux-clause (when (not (null aux))
		       (multiple-value-list
			(apply aux join-list))))
	 (recursive-clause (when (not (null recursive))
			     (multiple-value-list
			      (apply recursive join-list))))
	 (select-list (if (not (null select))
			  (multiple-value-list
			   (apply select join-list))
			  roots))
	 (where-clause (when (not (null where))
			 (multiple-value-list
			  (apply where join-list))))
	 (having-clause (when (not (null having))
			  (multiple-value-list
			   (apply having select-list))))
	 (order-by-clause (when (not (null order-by))
			    (multiple-value-list
			     (apply order-by join-list))))
	 (joining (compute-join-list
		   (append join-list aux-clause recursive-clause
			   select-list where-clause having-clause
			   order-by-clause)
		   aux-clause
		   recursive-clause)))
    (make-instance 'selection
		   :joining joining
		   :select-list (mapcar #'(lambda (select-item)
					    (select-item select-item))
					select-list)
		   :where-clause where-clause
		   :having-clause having-clause
		   :order-by-clause order-by-clause
		   :limit limit
		   :offset offset)))

(defun make-fetching (selection fetch-clause recursive-clause)
  (let ((fetching
	 (make-instance 'fetching
			:fetch-clause fetch-clause
			:selection (if (not (null (or
						   (limit-of selection)
						   (offset-of selection)
						   recursive-clause)))
				       (make-cte "selection" selection)
				       selection))))
    (if (not (null recursive-clause))
	(make-cte "fetching" fetching recursive-clause)
	fetching)))

(defun ensure-fetch (selection fetch)
  (let ((fetch-clauses (when (not (null fetch))
			 (multiple-value-list
			  (apply fetch (select-list-of selection))))))
    (if (not (null fetch))
	(make-fetching selection fetch-clauses
		       (mapcar #'recursive-selection-of fetch-clauses))
	selection)))

;;(defun make-joined-list (joined-list cte)
;;  (reduce #'(lambda (joined-list class-node)
;;	      (substitute
;;	       (recursive class-node cte) class-node joined-list))
;;	  (remove-if #'keywordp joined-list)
;;	  :initial-value joined-list))

;;(defmethod initialize-instance :after ((instance common-table-expression)
;;				       &key recursive query)
;;  (let ((*common-table-expression* instance))
;;    (with-slots (recursive-clause)
;;	instance
;;      (setf recursive-clause
;;	    (when (not (null recursive))
;;	      (multiple-value-list
;;		(apply recursive (from-clause-of query))))))))

(defun make-query (roots join &key select aux recursive where order-by
				having fetch limit offset)
  (let ((*table-index* 0))
    (ensure-fetch
     (compute-selection roots join aux recursive select where having
			order-by limit offset) fetch)))

(defun db-read (roots &key join aux recursive where order-by having
			select fetch singlep offset limit transform)
  (declare (ignore transform singlep))
  (make-query roots join
	      :aux aux
	      :recursive recursive
	      :select select
	      :where where
	      :order-by order-by
	      :having having
	      :offset offset
	      :limit limit
	      :fetch fetch))
