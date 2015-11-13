(in-package :cl-db)

(defun list-previous-contexts (context)
  (let ((previous-context
	 (previous-context-of context)))
    (when (not (null previous-context))
      (list* previous-context
	     (list-previous-contexts context)))))

;;(defun list-class-nodes (context)
;;   (reduce #'(lambda (from-clause class-nodes)
;;	       (append (mapcar #'compute-from-clause-expression class-nodes)
;;		       from-clause))
;;	   (hash-table-alist
;;	    (class-nodes-of context))
;;	   :key #'rest :initial-value nil))

;;(defun list-class-nodes (context)
;;  (mapcar #'(lambda (nodes)
;;	      (let ((root (first nodes)))
;;	      (list* root "---" (table-name-of (class-mapping-of root))
;;		     (mapcar #'(lambda (node)
;;				 (list node "---" (table-name-of (class-mapping-of node))))
;;			     (rest nodes)))))
;;	  (hash-table-alist
;;	   (class-nodes-of context))))

;;(defgeneric append-class-node (class-node &optional from-clause))

;;(defmethod append-class-node ((class-node root-node) &optional from-clause)
;;  (when (not (find class-node class-nodes))
;;    (list* root-node class-nodes)))

;;(defmethod append-class-node ((class-node superclass-node) &optional from-clause)
;;  (
;;  (list-class-nodes (supurclass-nodes-of class-node) class-nodes))
;;  (append-class-node
;;   (reduce #'append-class-node subclass-node-of class-node)
;;   (list* class-node (remove class-node class-nodes))))

;;(defmethod append-class-node ((class-node subclass-node) &optional from-clause)
;;  ())

;;(defmethod append-class-node ((class-node reference-node) &optional from-clause)
;;  ())

(defun list-column (column)
  (list "~a.~a"
	(list
	 (table-alias-of (correlation-of column))
	 (name-of column))))

(defgeneric list-select-item-expression (expression))

(defmethod list-select-item-expression ((expression column))
  (list-column expression))

(defun list-select-item (select-item)
  (destructuring-bind (expression . alias)
      select-item
    (list (list-select-item-expression expression) alias)))

(defun list-select-items (context)
  (mapcar #'list-select-item
	  (hash-table-alist
	   (expression-aliases-of context))))

(defun list-column-pair (column-pair)
  (list "~<~? = ~?~>"
	(append
	 (list-column
	  (first column-pair))
	 (list-column
	  (second column-pair)))))

(defun list-foreign-key (class-node)
  (mapcar #'list-column-pair (foreign-key-of class-node)))

(defgeneric list-class-node (class-node))

(defmethod list-class-node ((class-node root-node))
  (list "~a AS ~a"
	(list (table-name-of (class-mapping-of class-node))
	      (table-alias-of class-node))))

(defmethod list-class-node ((class-node reference-node))
  (list "~1TLEFT JOIN ~a AS ~a~%~7T ON ~{~{~?~}~^~%~7TAND ~}"
	(list (table-name-of (class-mapping-of class-node))
	      (table-alias-of class-node)
	      (list-foreign-key class-node))))

(defmethod list-class-node ((class-node subclass-node))
  (list "~1TLEFT JOIN ~a AS ~a~%~7T ON ~{~{~?~}~^~%~7TAND ~}"
	(list (table-name-of (class-mapping-of class-node))
	      (table-alias-of class-node)
	      (list-foreign-key class-node))))

(defmethod list-class-node ((class-node superclass-node))
  (list "~1TLEFT JOIN ~a AS ~a~%~7T ON ~{~{~?~}~^~%~7TAND ~}"
	(list (table-name-of (class-mapping-of class-node))
	      (table-alias-of class-node)
	      (list-foreign-key class-node))))

(defun list-class-nodes (root-list)
  (reverse
   (mapcar #'list-class-node
	   (rest root-list))))

(defun get-root-node (root)
  (gethash (class-mapping-of root) (class-nodes-of root)))

(defun list-from-clause (context)
  (let ((from-clause (make-hash-table)))
    (dolist (node-list (hash-table-alist (class-nodes-of context)))
      (let ((root (first node-list))
	    (class-nodes (rest node-list)))
	(if (eq (context-of (get-root-node root)) context)
	    (setf (gethash root from-clause)
		  class-nodes)
	    (setf (gethash (previous-context-of context) from-clause)
		  (append (gethash (previous-context-of context) from-clause)
			  class-nodes)))))
    (mapcar #'list-class-nodes
	    (reverse (hash-table-alist from-clause)))))

(defun list-list (list context)
  (mapcar #'(lambda (expression)
	      (list-expression expression context))
	  list))

(defun get-path-node (root path)
  (reduce #'(lambda (class-node superclass-mapping)
	      (get-path-node class-node superclass-mapping))
	  path
	  :from-end t
	  :initial-value (get-root-node root)))

(defgeneric list-expression (expression context))

(defmethod list-expression ((expression conjunction) context)
  (list "~{~{~?~}~^ AND ~}" (list (list-list
				   (arguments-of expression) context))))

(defmethod list-expression ((expression equality) context)
  (list "~? = ~?"
	(append
	 (list-expression (lhs-expression-of expression) context)
	 (list-expression (rhs-expression-of expression) context))))

(defmethod list-expression ((property-slot property-slot) context)
  (list-column
   (get-node-column context
		    (get-path-node
		     (root-of property-slot)
		     (path-of property-slot))
		    (column-of
		     (property-mapping-of property-slot)))))

(defmethod list-expression ((simple-value string) context)
  (declare (ignore context))
  (list "'~a'" (list simple-value)))

(defmethod list-expression ((simple-value number) context)
  (declare (ignore context))
  (list "~a" (list simple-value)))

(defun list-where-clause (context)
  (list "~{~{~?~}~^ AND ~}" (list
			     (list-list
			      (where-clause-of context) context))))

;; топологическая сортировка с учетом прошлого контекста

(defun write-sql-query (stream context)
  (format stream
	  (concatenate 'string
		       "~4TSELECT ~{~{~{~?~} AS ~a~}~^,~%~11T~}"
		       "~%~6TFROM ~{~{~{~?~}~^~%~}~^~%CROSS JOIN ~}"
		       " ~@[~%~5TWHERE ~{~?~}~]"
		       " ~@[~%GROUP BY ~a~]"
		       "~@[~%HAVING ~a~]"
		       "~@[~%ORDER BY ~a~]"
		       "~@[~%LIMIT ~a~]"
		       "~@[~%OFFSET ~a~]")
	  (list-select-items context)
	  (list-from-clause context)
	  (list-where-clause context)
	  (when (group-by-present-p context)
	    (class-node-columns-of context))
	  (having-clause-of context)
	  (order-by-clause-of context)
	  (limit-clause-of context)
	  (offset-clause-of context)))

(defun write-auxiliary-statements (stream contexts)
  (format stream "~@[WITH ~{~a~^,~%~}~]" contexts))

(defun write-query (stream context)
  (let ((previous-contexts (list-previous-contexts context)))
    (when (not (null previous-contexts))
      (write-auxiliary-statements stream previous-contexts))
    (write-sql-query stream context)))

(defun compile-sql-query (context)
  (with-open-stream (query-stream (make-string-output-stream))
    (write-query query-stream context)
    (get-output-stream-string query-stream)))

    
    
