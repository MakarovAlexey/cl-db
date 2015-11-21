(in-package :cl-db)

(defun list-previous-contexts (context)
  (let ((previous-context
	 (previous-context-of context)))
    (when (not (null previous-context))
      (list* previous-context
	     (list-previous-contexts previous-context)))))

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
	 (alias-of (correlation-of column))
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
	  (second column-pair))
	 (list-column
	  (first column-pair)))))

(defun list-foreign-key (class-node)
  (mapcar #'list-column-pair (foreign-key-of class-node)))

(defgeneric list-class-node (class-node))

(defmethod list-class-node ((class-node root-node))
  (list "~a AS ~a"
	(list (table-name-of (class-mapping-of class-node))
	      (alias-of class-node))))

(defmethod list-class-node ((context context))
  (list "~a AS ~a"
	(list (alias-of context)
	      (alias-of context))))

(defmethod list-class-node ((class-node reference-node))
  (list "~1TLEFT JOIN ~a AS ~a~%~7T ON ~{~{~?~}~^~%~7TAND ~}"
	(list (table-name-of (class-mapping-of class-node))
	      (alias-of class-node)
	      (list-foreign-key class-node))))

(defmethod list-class-node ((class-node subclass-node))
  (list "~1TLEFT JOIN ~a AS ~a~%~7T ON ~{~{~?~}~^~%~7TAND ~}"
	(list (table-name-of (class-mapping-of class-node))
	      (alias-of class-node)
	      (list-foreign-key class-node))))

(defmethod list-class-node ((class-node superclass-node))
  (list "~1TLEFT JOIN ~a AS ~a~%~7T ON ~{~{~?~}~^~%~7TAND ~}"
	(list (table-name-of (class-mapping-of class-node))
	      (alias-of class-node)
	      (list-foreign-key class-node))))

(defun list-class-nodes (root-list)
  (list* (list-class-node (first root-list))
	 (reverse
	  (mapcar #'list-class-node (rest root-list)))))

(defun get-root-node (root)
  (gethash (class-mapping-of root) (class-nodes-of root)))

(defun list-from-clause (context)
  (let ((from-clause (make-hash-table))
	(previous-context (previous-context-of context)))
    (dolist (node-list (hash-table-alist (class-nodes-of context)))
      (let ((root-node (get-root-node (first node-list)))
	    (class-nodes (rest node-list)))
	(if (eq (context-of root-node) context)
	    (setf (gethash root-node from-clause)
		  (remove root-node class-nodes))
	    (setf (gethash previous-context from-clause)
		  (append (gethash previous-context from-clause)
			  class-nodes)))))
    (when (not (null previous-context))
      (ensure-gethash previous-context from-clause nil))
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

(defgeneric list-equality-expression (lhs-expression rhs-expression context))

(defmethod list-equality-expression ((lhs-expression root)
				     rhs-expression context)
  (let ((lhs-node (get-root-node lhs-expression))
	(rhs-node (get-root-node rhs-expression)))
    (list "(~{~a = ~a~^AND ~})"
	  (list
	   (mapcar #'(lambda (lhs-column rhs-column)
		       (list
			(list-column
			 (get-node-column context lhs-node lhs-column))
			(list-column
			 (get-node-column context rhs-node rhs-column))))
		   (primary-key-of
		    (class-mapping-of lhs-node))
		   (primary-key-of
		    (class-mapping-of rhs-node)))))))

(defmethod list-equality-expression (lhs-expression rhs-expression context)
  (list "~? = ~?"
	(append
	 (list-expression lhs-expression context)
	 (list-expression rhs-expression context))))

(defmethod list-expression ((expression equality) context)
  (list-equality-expression (lhs-expression-of expression)
			    (rhs-expression-of expression)
			    context))

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

;;(defmethod list-expression ((fetched-reference fetched-reference) context)
;;  (

(defun list-where-clause (context)
  (list "~{~{~?~}~^ AND ~}" (list
			     (list-list
			      (where-clause-of context) context))))

;; топологическая сортировка с учетом прошлого контекста

(defun list-sql-query (context)
  (list (concatenate 'string
		     "~4TSELECT ~{~{~{~?~} AS ~a~}~^,~%~11T~}"
		     "~%~6TFROM ~{~{~{~?~}~^~%~}~^~%CROSS JOIN ~}"
		     " ~@[~%~5TWHERE ~{~?~}~]"
		     " ~@[~%GROUP BY ~a~]"
		     "~@[~%HAVING ~a~]"
		     "~@[~%ORDER BY ~a~]"
		     "~@[~%LIMIT ~a~]"
		     "~@[~%OFFSET ~a~]")
	(list
	 (list-select-items context)
	 (list-from-clause context)
	 (when (not (null (where-clause-of context)))
	   (list-where-clause context))
	 (when (group-by-present-p context)
	   (class-node-columns-of context))
	 (having-clause-of context)
	 (order-by-clause-of context)
	 (limit-clause-of context)
	 (offset-clause-of context))))

(defun recursivep (context)
  (not (null (recursive-clause-of context))))

(defgeneric list-recursive-clause (expression context))

(defmethod list-recursive-clause ((expression fetched-reference) context)
  (let ((root-node
	 (get-root-node expression))
	(recursive-node
	 (get-root-node
	  (recursive-node-of expression))))
    (list "(~{~a.~a = ~?~^~%~6TAND ~})"
;;	  (reduce #'list*
		  (mapcar #'(lambda (lhs-column rhs-column)
			      (list*
			       (alias-of context)
			       (select-column context recursive-node rhs-column)
			       (list-column
				(get-node-column context root-node lhs-column))))
			  (primary-key-of
			   (class-mapping-of root-node))
			  (primary-key-of
			   (class-mapping-of recursive-node)))))))

(defun list-recursive-part (context)
  ;; WHERE-clause - recursive-clause, JOIN - recursive fetch ?
  (list
   (concatenate 'string
		"~4TSELECT ~{~{~{~?~} AS ~a~}~^,~%~11T~}"
		"~%~6TFROM ~{~{~{~?~}~^~%~}~^~%CROSS JOIN ~}"
		"~%~6TCROSS JOIN ~a~%~8TWHERE ~{~?~^~%~8TOR ~}")
   (list
    (list-select-items context)
    (list-from-clause context)
    (alias-of context)
    (reduce #'append (recursive-clause-of context)
	    :key #'(lambda (recursive-expression)
		     (list-recursive-clause recursive-expression
					    context))))))

(defun list-auxiliary-statement (context)
  (if (recursivep context)
      (list "~a (~<~{~a~^, ~}~@:>) AS (~%~?~%~5TUNION ALL~%~?~%~6T)"
	    (list* (alias-of context)
		   (list
		    (mapcar #'rest (hash-table-alist
				    (expression-aliases-of context))))
		   (append
		    (list-sql-query context)
		    (list-recursive-part context))))
      (list "~a AS (~%~?~%~6T)"
	    (list* (alias-of context)
		   (list-sql-query context)))))

(defun list-query (context)
  (let ((previous-contexts (reverse (list-previous-contexts context))))
    (list
     (if (some #'recursivep previous-contexts)
	 "~@[~6TWITH RECURSIVE ~{~{~?~}~^, ~}~]~%~?"
	 "~@[~6TWITH ~{~{~?~}~^, ~}~]~%~?")
     (list*
      (when (not (null previous-contexts))
	(mapcar #'list-auxiliary-statement previous-contexts))
      (list-sql-query context)))))

(defun compile-sql-query (context)
  (with-open-stream (query-stream (make-string-output-stream))
    (format query-stream "~%~{~?~}" (list-query context))
    (get-output-stream-string query-stream)))
