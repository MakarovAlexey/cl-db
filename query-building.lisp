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
  ((expressions :initarg :expressions
		:reader expressions-of)
   (from-clause :reader from-clause-of)
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

(defgeneric append-joining-nodes (from-clause expression))

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
					 (reference-slot-of expression)))
		      expression))

(defvar *table-index*)

(defun make-alias (&rest name-parts)
  (format nil "~{~(~a~)_~}~a" name-parts (incf *table-index*)))

(defmethod initialize-instance ((instance joining)
				&key expressions &allow-other-keys)
  (with-slots (from-clause table-aliases)
      instance
    (setf from-clause
	  (reduce #'append-joining-nodes
		  expressions :initial-value nil))
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
		     (make-alias
		      (class-name-of
		       (class-mapping-of select-item)))
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
	(make-instance 'auxiliary-selection :joining joining
		       :select-list select-list
		       :order-by order-by
		       :having having
		       :offset offset
		       :limit limit
		       :where where)
	(make-instance 'selection :joining joining
		       :select-list select-list
		       :order-by order-by
		       :having having
		       :offset offset
		       :limit limit
		       :where where)))

(defun make-fetching (selection fetch-references recursive-fetch-references)
  (if (not (null recursive-fetch-references))
      (make-instance 'recursive-fetching
		     :recursive-fetch-references recursive-fetch-references
		     :fetch-references fetch-references
		     :selection selection)
      (make-instance 'fetching
		     :selection selection
		     :fetch fetch-references)))

(defun ensure-fetching (selection fetch-reference recursive-fetch-references)
  (if (not (null fetch-reference))
      (make-fetching selection fetch-reference recursive-fetch-references)
      selection))

(defun make-query (roots join &key select aux recursive where order-by
				having fetch limit offset)
  (let* ((*table-index* 0)
	 (selectors (if (listp roots)
			(mapcar #'(lambda (root)
				    (make-root root))
				roots)
			(list (make-root roots))))
	 (join-list (multiple-value-call #'append selectors
					 (when (not (null join))
					   (apply join selectors))))
	 (select-items (if (not (null select))
			   (multiple-value-list
			    (apply select join-list))
			   selectors))
	 (aux-clause (when (not (null aux))
		       (multiple-value-list
			(apply aux join-list))))
	 (recursive-clause (when (not (null recursive))
			     (multiple-value-list
			      (apply recursive join-list))))
	 (joining (make-joining (append select-items where having
					order-by aux recursive-clause)
				aux-clause recursive-clause))
	 (select-list (mapcar #'(lambda (select-item)
				  (select-item select-item joining))
			      select-items))
	 (reference-fetchings
	  (when (not (null fetch))
	    (multiple-value-list
	     (apply fetch select-list))))
	 (recursive-fetch-references
	  (remove-if #'null reference-fetchings
		     :key #'recursive-node-of))
	 (selection
	  (make-selection joining
			  (or limit offset recursive-fetch-references)
			  select-list where having order-by limit offset)))
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
