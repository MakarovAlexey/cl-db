(in-package :cl-db)

(defvar *query-output*)

(defun list-previous-contexts (context)
  (let ((previous-context
	 (previous-context-of context)))
    (when (not (null previous-context))
      (list* previous-context
	     (list-previous-contexts context)))))

(defun write-select-item (stream object colon at-sign)
  (declare (ignore colon at-sign))
  (destructuring-bind (expression . alias)
      object
    (format stream "~a AS ~a" expression alias)))

(defun list-class-nodes (context)
  (hash-table-alist
   (class-nodes-of context)))

(defun write-sql-query (stream context)
  (format stream
	  (concatenate 'string
		       "SELECT ~{~W~^,~%~7T~}~%"
		       "  FROM ~a~%"
		       " ~@[WHERE ~a~%~]"
		       " ~@[GROUP BY ~a~%~]"
		       "~@[HAVING ~a~%~]"
		       "~@[ORDER BY ~a~%~]"
		       "~@[LIMIT ~a~%~]"
		       "~@[OFFSET ~a~%~]")
	  (hash-table-alist (expression-aliases-of context))
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

    
    
