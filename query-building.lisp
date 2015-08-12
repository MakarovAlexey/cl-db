(in-package #:cl-db)

;;; Query building

;;;; Joining

(defclass joining ()
  ((expressions :initarg :expressions
		:reader expressions-of)
   (from-clause :reader from-clause-of)))

(defun ensure-class-node (class-node from-clause)
  (if (not (find superclass-node from-clause))
      (list* superclass-node from-clause)
      from-clause))

(defgeneric append-path-node (from-clause class-node))

(defmethod append-path-node (from-clause (superclass-node superclass-node))
  (append-path-node
   (ensure-class-node superclass-node from-clause)
   (class-node-of superclass-node)))

(defmethod append-path-node (from-clause (root-node root-node))
  (ensure-class-node root-node from-clause))

(defun append-class-nodes (from-clause class-node)
  (reduce #'ensure-class-node
	  (precedence-list-of expression)
	  :from-end t
	  :initial-value (ensure-class-node class-node from-clause)))

(defun append-property-nodes (from-clause property-slot)
  (append-path-node from-clause (class-node-of property-slot)))

(defgeneric append-aggregation-nodes (from-clause expression))

(defmethod append-aggregation-nodes (from-clause (expression expression))
  (reduce #'append-aggregation-nodes (arguments-of expression)
	  :initial-value from-clause))

(defmethod append-aggregation-nodes (from-clause (expression property-slot))
  (append-property-nodes expression from-clause))

(defmethod append-aggregation-nodes (from-clause (expression root-class-selection))
  (ensure-class-node
   (concrete-class-node-of expression) from-clause))

(defmethod append-aggregation-nodes (from-clause (expression reference-node))
  (ensure-class-node
   (concrete-class-node-of expression)
   (append-path-node from-clause
		     (class-node-of
		      (reference-slot-of expression)))))

(defgeneric append-joining-nodes (expression))

(defmethod append-joining-nodes (from-clause (expression expression))
  (reduce #'append-joining-nodes (arguments-of expression)
	  :initial-value from-clause))

(defmethod append-joining-nodes (from-clause (expression aggregation))
  (reduce #'append-aggregation-nodes (arguments-of expression)
	  :initial-value from-clause))

(defmethod append-joining-nodes (from-clause (expression property-slot))
  (append-property-nodes expression from-clause))

(defmethod append-joining-nodes (from-clause (expression root-node))
  (append-class-nodes from-clause expression))

(defmethod append-joining-nodes (from-clause (expression reference-node))
  (append-class-nodes (append-path-node from-clause
					(class-node-of
					 (reference-slot-slot expression)))
		      expression))

(defmethod initialize-instance ((instance joining)
				&key expressions &allow-other-keys)
  (with-slots (from-clause)
      instance
    (setf from-clause
	  (reduce #'append-joining-nodes
		  expressions :initial-value nil))))

;; названия колонок известны, но неизвестны псевдонимы таблиц
(defclass simple-joining (joining)
  ((table-alaises :reader table-aliases-of)))

;;(defun get-table-alias (class-node table-aliases)
;;  (rest (assoc class-node table-aliases)))

(defvar *table-index*)

(defun make-alias (&rest name-parts)
  (format nil "~{~(~a~)_~}~a" name-parts (incf *table-index*)))

(defmethod initialize-instance :after ((instance simple-joining)
				       &key &allow-other-keys)
  (with-slots (table-aliases)
      instance
    (setf table-aliases
	  (mapcar #'(lambda (class-node)
		      (make-alias
		       (class-name-of
			(class-mapping-of class-node))))
		  (from-clause-of instance)))))

(defclass column-selection ()
  ((column-name :initarg :column-name
		:reader column-name-of)
   (class-node :initarg :class-node
	       :reader class-node)
   (alias :initarg :alias
	  :reader alias-of)))

(defun select-column (column-name class-node alias)
  (make-instance 'column-selection
		 :column-name column-name
		 :class-node class-node
		 :alias alias))

;; известен псевдоним табицы, но неизвестны названия колонок
(defclass recursive-joining (joining)
  ((node-columns :reader column-aliases)
   (recursive-clause :initarg :recursive-clause
		     :reader recursive-clause-of)
   (aux-clause :initarg :aux-clause
	       :reader aux-clause)))

(defun compute-node-columns (class-node)
  (mapcar #'(lambda (column-name)
	      (select-column column-name class-node
			     (make-alias column-name)))
	  (columns-of
	   (class-mapping-of class-node))))

(defgeneric compute-joining-column-aliases (expression))

(defmethod compute-joining-column-aliases ((root-node root-node))
  (reduce #'(lambda (class-node result)
	      (acons class-node
		     (compute-node-columns class-node)
		     result))
	  (precedence-list-of root-node)
	  :from-end t
	  :initial-value nil))

(defmethod compute-joining-column-aliases ((property-slot property-slot))
  (let ((property-mapping (property-mapping-of property-slot)))
    (select-column
     (column-of property-mapping)
     (class-node-of property-slot)
     (make-alias
      (slot-name-of property-mapping)))))

(defmethod compute-joining-column-aliases ((expression expression))
  (mapcar #'compute-joining-column-aliases (arguments-of expression)))

(defmethod initialize-instance :after ((instance recursive-joining)
				       &key expressions &allow-other-keys)
  (with-slots (column-aliases)
      instance
    (setf column-aliases
	  (reduce #'(lambda (result expression)
		      (acons expression
			     (compute-joining-column-aliases expression)
			     result))
		  expressions :initial-value nil))))

;; Selection

(defclass selection ()
  ((joining :initarg :joining
	    :reader joining-of)
   (select-list :initarg :select-list
		:reader select-list-of)
   (where-clause :initarg :where-clause
		 :reader where-clause-of)
   (having-clause :initarg :having-clause
		  :reader having-clause-of)
   (limit :initarg :limit
	  :reader limit-of)
   (offset :initarg :offset
	   :reader offset-of)
   (order-by-clause :initarg :order-by-clause
		    :reader order-by-clause-of)
   (table-aliases :initarg :table-aliases
		  :reader table-aliases-of)))

(defgeneric select-item (expression joining))

(defmethod select-item ((expression property-slot) joining)
  (make-instance 'property-selection
		 :alias (make-alias
			 (slot-name-of
			  (mapping-of expression)))
		 :property-slot expression
		 :joining joining))

(defmethod select-item ((expression expression) joining)
  (make-instance 'expression-selection
		 :alias (make-alias "expr")
		 :expression expression
		 :joining joining))

(defmethod select-item ((expression root-node) joining)
  (make-instance 'root-class-selection
		 :class-mapping (class-mapping-of expression)
		 :concrete-class-node expression
		 :joining joining))

(defgeneric compute-selection-table-aliases (select-item))

(defmethod compute-selection-table-aliases ((select-item class-selection))
  (reduce #'(lambda (result class-node)
	      (acons class-node
		     (compute-node-columns class-node)
		     result))
	  (class-nodes-of select-item)
	  :initial-value nil))

(defmethod compute-selection-table-aliases ((select-item simple-selection))
  (declare (ignore select-item)))

(defmethod initialize-instance :after ((instance selection)
				       &key joining select-list
					 &allow-other-keys)
  (with-slots (select-items table-aliases)
      instance
    (setf select-items
	  (mapcar #'(lambda (select-item)
		      (select-item select-list joining))
		  select-list))
    (setf table-aliases
	  (reduce #'append select-items
		  :key #'(lambda (select-item)
			   (compute-selection-table-aliases select-item))))))

(defclass auxiliary-selection (selection)
  ((subclass-columns :reader subclass-columns-of)))

;; вычислять какие либо колонки для auxiliary-select --- нет
;; необходимости, так как сами selection-items "знают" все псевдонимы
;; для их выборки

;;;; Fetching

(defclass fetching ()
  ((selection :initarg :selection
	      :reader selection-of)
   (fetch-clause :initarg :fetch-clause
		 :reader fetch-clause-of)))




(defclass recursive-class-node ()
  ((common-table-expression :initarg :common-table-expression
			    :reader common-table-expression-of)
   (class-node :initarg :class-node
	       :reader class-node-of)))





(defclass common-table-expression ()
  ((name :initarg :name
	 :reader name-of)
   (query :initarg :query
	  :reader query-of)
   (recursive-clause :initarg :recursive-clause
		     :reader recursive-clause-of)))

(defclass sql-query ()
  ((select-list)
   (from-clause)
   (where-clause)
   (group-by-clause)
   (having-clause)
   (order-by)
   (limit)
   (offset)))

(defclass column-plan ()
  ((column-name :initarg :column-name
		:reader column-name-of)
   (class-node :initarg :class-node
	       :reader class-node-of)))


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

(defmethod join-list-of ((cte common-table-expression))
  (join-list-of (query-of cte)))

(defmethod roots-of ((cte common-table-expression))
  (roots-of (query-of cte)))



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
  (reduce #'append (arguments-of expression) :key #'get-columns))

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

(defgeneric get-joining-column (joining class-node column-name))

(defmethod get-joining-column ((joining recursive-joining) class-node column-name)
  (cons (name-of joining)
	(rest (find-if #'(lambda (column-plan)
			   (and (eq (class-node-of column-plan) class-node)
				(eq (column-name-of column-plan) column-name)))
		       (columns-of joining)
		       :key #'first))))

(defmethod get-joining-column ((joining joining) class-node column-name)
  (cons (table-alias class-node (table-aliases-of joining))
	column-name))

(defgeneric get-column (query-object column-name))

(defmethod get-column ((class-selection root-class-selection) column-name)
  (get-joining-column (joining-of class-selection)
		     (concrete-class-node-of class-selection)
		     column-name))

(defmethod get-column ((subclass-node subclass-node) column-name)
  (cons (alias-of subclass-node) column-name))

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



(defun compute-from-clause (query-object)
  (reduce #'append (subclass-nodes-of query-object)
	  :key #'(lambda (subclass-node)
		   (let ((subclass-mapping
			  (class-mapping-of subclass-node))
			 (alias
			  (alias-of subclass-node)))
		     (list* :left-join (table-name-of
					(class-mapping-of subclass-node))
			    :as alias
			    :on (mapcar #'(lambda (pk-column fk-column)
					      (list
					       (get-column pk-column subclass-node)
					       (get-column fk-column query-object)))
					(primary-key-of subclass-mapping)
					(foreign-key-of subclass-mapping))
			    (compute-from-clause subclass-node))))
	    :initial-value nil))

(defun make-selection (joining select-list where-clause having-clause
		       order-by-clause limit offset)
  (make-instance 'selection
		 :joining joining
		 :select-list select-list
		 :from-clause (reduce #'append select-list
				      :key #'compute-from-clause
				      :initial-value (from-clause-of joining))
		 :where-clause where-clause
		 :having-clause having-clause
		 :order-by-clause order-by-clause
		 :limit limit
		 :offset offset))

(defun make-auxullary-selection (selection)
  (make-instance 'auxullary-selection :selection selection))

(defun compute-selection (roots join aux recursive select where having
			  order-by limit offset fetch)
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
		   recursive-clause))
	 (selection
	  (make-selection joining (mapcar #'(lambda (select-item)					      (select-item select-item joining))
					  select-list)
			  where-clause having-clause order-by-clause
			  limit offset))
	 (fetch-clauses (when (not (null fetch))
			  (multiple-value-list
			   (apply fetch (select-list-of selection))))))
    (if (not (or (and (or limit offset) fetch-clauses)
		 (some #'recursive-selection-of fetch-clauses)))
	selection
	(make-auxullary-selection selection))))

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
    (compute-selection roots join aux recursive select where having
		       order-by limit offset fetch) fetch))

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
