(in-package #:cl-db)

;;; Query building

(defclass column-plan ()
  ((column-name :initarg :column-name
		:reader column-name-of)
   (class-node :initarg :class-node
	       :reader class-node-of)))

;;;; Joining

;; названия колонок известны, но неизвестны псевдонимы таблиц
(defclass joining ()
  ((from-clause :reader from-clause-of)
   (table-aliases :reader table-aliases-of)))

;;(defun get-table-alias (class-node table-aliases)
;;  (rest (assoc class-node table-aliases)))

(defun ensure-class-node (class-node from-clause)
  (if (not (find class-node from-clause))
      (list* class-node from-clause)
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
	  (precedence-list-of class-node)
	  :from-end t
	  :initial-value (ensure-class-node class-node from-clause)))

(defun append-property-nodes (from-clause property-slot)
  (append-path-node from-clause (class-node-of property-slot)))

(defgeneric append-aggregation-nodes (from-clause expression))

(defmethod append-aggregation-nodes (from-clause (expression expression))
  (reduce #'append-aggregation-nodes (arguments-of expression)
	  :initial-value from-clause))

(defmethod append-aggregation-nodes (from-clause (expression property-slot))
  (append-property-nodes from-clause expression))

(defmethod append-aggregation-nodes (from-clause (expression root-class-selection))
  (ensure-class-node
   (concrete-class-node-of expression) from-clause))

(defmethod append-aggregation-nodes (from-clause (expression reference-node))
  (ensure-class-node
   (concrete-class-node-of expression)
   (append-path-node from-clause
		     (class-node-of
		      (reference-slot-of expression)))))

(defgeneric append-joining-nodes (from-clause expression))

(defmethod append-joining-nodes (from-clause (expression n-ary-expression))
  (reduce #'append-joining-nodes (arguments-of expression)
	  :initial-value from-clause))

(defmethod append-joining-nodes (from-clause (expression binary-operation))
  (append-joining-nodes
   (append-joining-nodes from-clause (lhs-expression-of expression))
   (rhs-expression-of expression)))

(defmethod append-joining-nodes (from-clause (expression aggregation))
  (reduce #'append-aggregation-nodes (arguments-of expression)
	  :initial-value from-clause))

(defmethod append-joining-nodes (from-clause (value t))
  (declare (ignore value))
  from-clause)

(defmethod append-joining-nodes (from-clause (expression property-slot))
  (append-property-nodes from-clause expression))

(defmethod append-joining-nodes (from-clause (expression root-node))
  (append-class-nodes from-clause expression))

(defmethod append-joining-nodes (from-clause (expression reference-node))
  (append-class-nodes (append-path-node from-clause
					(class-node-of
					 (reference-slot-of expression)))
		      expression))

(defvar *table-index*)

(defun make-alias (&rest name-parts)
  (format nil "~{~(~a~)_~}~a" name-parts (incf *table-index*)))

(defmethod initialize-instance :after ((instance joining)
				       &key expressions &allow-other-keys)
  (with-slots (from-clause table-aliases)
      instance
    (setf from-clause
	  (reduce #'append-joining-nodes
		  expressions :initial-value nil))
    (setf table-aliases
	  (reduce #'(lambda (result class-node)
		      (acons class-node
			     (make-alias
			      (class-name-of
			       (class-mapping-of class-node)))
			     result))
		  (from-clause-of instance)
		  :initial-value nil))))

(defclass column-selection ()
  ((column-name :initarg :column-name
		:reader column-name-of)
   (class-node :initarg :class-node
	       :reader class-node-of)
   (alias :initarg :alias
	  :reader alias-of)))

(defun ensure-column (selected-columns column-name class-node)
  (if (find-if-not #'(lambda (selected-column)
		       (and (eq (class-node-of selected-column)
				class-node)
			    (eq (column-name-of selected-column)
				column-name)))
		   selected-columns)
      (list* (make-instance 'column-selection
			    :column-name column-name
			    :class-node class-node
			    :alias (make-alias column-name))
	     selected-columns)
      selected-columns))

;; известен псевдоним табицы, но неизвестны названия колонок
(defclass recursive-joining (joining)
  ((recursive-clause :initarg :recursive-clause
		     :reader recursive-clause-of)
   (aux-clause :initarg :aux-clause
	       :reader aux-clause)
   (node-columns :reader column-aliases)))

(defun compute-class-node-columns (select-columns class-node)
  (reduce #'(lambda (result column-name)
	      (ensure-column result column-name class-node))
	  (columns-of
	   (class-mapping-of class-node))
	  :initial-value select-columns))

(defgeneric compute-joining-column-aliases (expression result))

(defmethod compute-joining-column-aliases ((root-node root-node) result)
  (reduce #'(lambda (class-node result)
	      (acons class-node
		     (compute-class-node-columns result class-node)
		     result))
	  (precedence-list-of root-node)
	  :from-end t
	  :initial-value nil))

(defmethod compute-joining-column-aliases ((property-slot property-slot) result)
  (let ((property-mapping (property-mapping-of property-slot)))
    (ensure-column result
		   (column-of property-mapping)
		   (class-node-of property-slot))))

(defmethod compute-joining-column-aliases ((expression n-ary-expression) result)
  (select-columns (arguments-of expression) result))

(defmethod compute-joining-column-aliases ((expression binary-operation) result)
  (compute-joining-column-aliases
   (rhs-expression-of expression)
   (compute-joining-column-aliases
    (lhs-expression-of expression) result)))

(defmethod compute-joining-column-aliases ((expression recursive-class-node) result)
  (compute-joining-column-aliases
   (rhs-expression-of expression)
   (compute-joining-column-aliases
    (lhs-expression-of expression) result)))

(defun select-columns (expressions &optional (selected-columns nil))
  (reduce #'compute-joining-column-aliases expressions
	  :initial-value selected-columns
	  :from-end t))

(defmethod initialize-instance :after ((instance recursive-joining)
				       &key expressions &allow-other-keys)
  (with-slots (column-aliases)
      instance
    (setf column-aliases (select-columns expressions))))

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

(defmethod compute-selection-table-aliases ((select-item root-class-selection))
  (reduce #'(lambda (result class-node)
	      (acons class-node
		     (make-alias
		      (class-name-of
		       (class-mapping-of class-node)))
		     result))
	  (class-nodes-of select-item)
	  :initial-value nil))

(defmethod compute-selection-table-aliases ((select-item simple-selection))
  (declare (ignore select-item)))

(defmethod initialize-instance :after ((instance selection)
				       &key select-list &allow-other-keys)
  (with-slots (table-aliases)
      instance
    (setf table-aliases
	  (reduce #'append select-list
		  :key #'(lambda (select-item)
			   (compute-selection-table-aliases select-item))))))

(defclass auxiliary-selection (selection)
  ())

;; вычислять какие либо колонки для auxiliary-select - нет
;; необходимости, так как сами selection-items "знают" все псевдонимы
;; для их выборки

;;;; Fetching

(defclass fetching ()
  ((selection :initarg :selection
	      :reader :selection-of)
   (table-aliases :initarg :table-aliases
		  :reader table-aliases-of)
   (reference-fetchings :initarg :reference-fetchings
			:reader reference-fetchings-of)))

(defmethod initialize-instance :after ((instance fetching)
				       &key reference-fetchings
					 &allow-other-keys)
  (let ((class-nodes
	 (reduce #'append reference-fetchings :key #'class-nodes-of)))
    (with-slots (table-aliases)
	instance
      (setf table-aliases
	    (reduce #'(lambda (result class-node)
			(acons class-node
			       (make-alias
				(class-name-of
				 (class-mapping-of class-node)))
			       result))
		    class-nodes
		    :initial-value nil)))))

(defclass recursive-fetching (fetching)
  ((recursive-fetch-references :initarg :recursive-fetch-references
			       :reader recursive-fetch-references-of)))

(defun make-joining (expressions aux recursive)
  (if (not (null recursive))
      (make-instance 'recursive-joining
		     :expressions expressions
		     :recursive recursive
		     :aux aux)
      (make-instance 'joining
		     :expressions expressions)))

(defun make-selection (joining auxiliaryp select-list where having
		       order-by limit offset)
  (if (not (null auxiliaryp))
      (make-instance 'auxiliary-selection
		     :joining joining
		     :select-list select-list
		     :order-by-clause order-by
		     :having-clause having
		     :where-clause where
		     :offset offset
		     :limit limit)
      (make-instance 'selection
		     :joining joining
		     :select-list select-list
		     :order-by-clause order-by
		     :having-clause having
		     :where-clause where
		     :offset offset
		     :limit limit)))

(defun make-fetching (selection fetch-references recursive-fetch-references)
  (if (not (null recursive-fetch-references))
      (make-instance 'recursive-fetching
		     :recursive-fetch-references recursive-fetch-references
		     :fetch-references fetch-references
		     :selection selection)
      (make-instance 'fetching
		     :fetch-references fetch-references
		     :selection selection)))

(defun ensure-fetching (selection fetch-reference recursive-fetch-references)
  (if (not (null fetch-reference))
      (make-fetching selection fetch-reference recursive-fetch-references)
      selection))

(defun compute-clause (clause args &optional default)
  (if (not (null clause))
      (multiple-value-list
       (apply clause args))
      default))

(defclass query ()
  ((select-list :initarg :select-list
		:reader select-list-of)
   (aux-clause :initarg :aux-clause
	       :reader aux-clause-of)
   (recursive-clause :initarg :recursive-clause
		     :reader recursive-clause-of)
   (order-by-clause :initarg :order-by-clause
		    :reader order-by-clause-of)
   (having-clause :initarg :having-clause
		  :reader having-clause-of)
   (where-clause :initarg :where-clause
		 :reader where-clause-of)
   (fetch-clause :initarg :fetch-clause
		 :reader fetch-clause-of)
   (table-aliases :initform (make-hash-table)
		  :reader table-aliases-of)
   (selected-columns :initform (make-hash-table)
		     :reader selected-columns)
   (joining-nodes :initarg :joining-nodes
		  :reader joining-nodes-of)
   (selection-nodes :initarg :selection-nodes
		    :reader selection-nodes-of)
   (fetching-nodes :initarg :fetching-nodes
		   :reader fetchoing-nodes-of)))

(defun make-query (roots join &key select aux recursive where order-by
				having fetch limit offset)
  (let* ((*table-index* 0)
	 (selectors (if (listp roots)
			(mapcar #'(lambda (root)
				    (make-root root))
				roots)
			(list (make-root roots))))
	 (join-list (reduce #'append (compute-clause join selectors)
			    :initial-value selectors))
	 (select-items (compute-clause select join-list selectors))
	 (recursive-clause (compute-clause recursive join-list))
	 (order-by-clause (compute-clause order-by join-list))
	 (having-clause (compute-clause having join-list))
	 (where-clause (compute-clause where join-list))
	 (aux-clause (compute-clause aux join-list))
	 (joining (make-joining (append select-items where-clause
					having-clause order-by-clause
					aux-clause recursive-clause)
				aux-clause recursive-clause))
	 (select-list (mapcar #'(lambda (select-item)
				  (select-item select-item joining))
			      select-items))
	 (reference-fetchings
	  (when (not (null fetch))
	    (reduce #'append (multiple-value-list
			      (apply fetch select-list)))))
	 (recursive-fetch-references
	  (remove-if #'null reference-fetchings
		     :key #'recursive-node-of))
	 (selection
	  (make-selection joining
			  (or limit offset recursive-fetch-references)
			  select-list where-clause having-clause
			  order-by-clause limit offset)))
    (ensure-fetching selection
		     reference-fetchings
		     recursive-fetch-references)))

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
