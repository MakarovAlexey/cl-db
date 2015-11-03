(in-package :cl-db)

(defun list-previous-contexts (context)
  (let ((previous-context
	 (previous-context-of context)))
    (when (not (null previous-context))
      (list* previous-context
	     (list-previous-contexts context)))))

(defgeneric print-expression (stream object)
  )

(defmethod print-expression (stream object))
 
(defun write-expression (stream object &optional colon at-sign)
  (declare (ignore colon at-sign))
  (format stream "~a" object))

(defun list-class-nodes (context)
  (mapcar #'(lambda (nodes)
	      (let ((root (first nodes)))
	      (list* root "---" (table-name-of (class-mapping-of root))
		     (mapcar #'(lambda (node)
				 (list node "---" (table-name-of (class-mapping-of node))))
			     (rest nodes)))))
	  (hash-table-alist
	   (class-nodes-of context))))

(defgeneric append-class-node (class-node &optional class-nodes))

(defmethod append-class-node ((class-node root-node) &optional class-nodes)
  (when (not (find class-node class-nodes))
    (list* root-node class-nodes)))

(defmethod append-class-node ((class-node superclass-node) &optional class-nodes)
  (append-class-node
   (subclass-node-of class-node)
   (list* class-node (remove class-node class-nodes))))

(defmethod append-class-node ((class-node subclass-node) &optional class-nodes)
  ())

(defmethod append-class-node ((class-node reference-node) &optional class-nodes)
  ())

(defun list-class-nodes (context)
  (reduce #'append-class-node
	  (hash-table-values
	   (class-nodes-of context)) :from-end t))

(defun write-sql-query (stream context)
  (format stream
	  (concatenate 'string
		       "   SELECT ~{~/cl-db:write-expression/ AS ~a~^,~%~10T~}"
		       "~%     FROM ~@[~a~%LEFT JOIN ~]~{~a~^~%LEFT JOIN ~}"
		       " ~@[~%    WHERE ~a~]"
		       " ~@[~%GROUP BY ~a~]"
		       "~@[~%HAVING ~a~]"
		       "~@[~%ORDER BY ~a~]"
		       "~@[~%LIMIT ~a~]"
		       "~@[~%OFFSET ~a~]")
	  (hash-table-plist
	   (expression-aliases-of context))
	  (previous-context-of context)
	  (list-class-nodes context)
	  (where-clause-of context)
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

    
    
