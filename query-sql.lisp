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

(defgeneric compute-from-clause-expression (class-node))

(defmethod compute-from-clause-expression (class-node)
  (list (table-name-of (class-mapping-of class-node))
	:as (table-alias-of class-node)))

;;(defun list-class-nodes (context)
;;   (reduce #'(lambda (from-clause class-nodes)
;;	       (append (mapcar #'compute-from-clause-expression class-nodes)
;;		       from-clause))
;;	   (hash-table-alist
;;	    (class-nodes-of context))
;;	   :key #'rest :initial-value nil))

(defun list-class-nodes (context)
  (mapcar #'(lambda (nodes)
	      (let ((root (first nodes)))
	      (list* root "---" (table-name-of (class-mapping-of root))
		     (mapcar #'(lambda (node)
				 (list node "---" (table-name-of (class-mapping-of node))))
			     (rest nodes)))))
	  (hash-table-alist
	   (class-nodes-of context))))

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
  (list
   (table-alias-of (correlation-of column))
   (name-of column)))

(defun list-column-pair (column-pair)
  (list (list-column
	 (first column-pair))
	(list-column
	 (second column-pair))))

(defun list-foreign-key (class-node)
  (mapcar #'list-column-pair (foreign-key-of class-node)))

(defgeneric list-class-node (class-node))

(defmethod list-class-node ((class-node root-node))
  (list (list (table-name-of (class-mapping-of class-node))
	      :as (table-alias-of class-node))))

(defmethod list-class-node ((class-node reference-node))
  (list (list :left :join (table-name-of (class-mapping-of class-node))
	      :as (table-alias-of class-node))
	(list-foreign-key class-node)))

(defmethod list-class-node ((class-node subclass-node))
  (list (list :left :join (table-name-of (class-mapping-of class-node))
	      :as (table-alias-of class-node))
	(list-foreign-key class-node)))

(defmethod list-class-node ((class-node superclass-node))
  (list (list :left :join (table-name-of (class-mapping-of class-node))
	      :as (table-alias-of class-node))
	(list-foreign-key class-node)))

(defun list-class-nodes (root-list)
  (reverse
   (mapcar #'list-class-node
	   (rest root-list))))

(defun list-from-clause (context)
  (mapcar #'list-class-nodes
	  (reverse (hash-table-alist
		    (class-nodes-of context)))))

(defun write-sql-query (stream context)
  (format stream
	  (concatenate 'string
		       "    SELECT ~{~/cl-db:write-expression/ AS ~a~^,~%~11T~}"
		       "~%~6TFROM ~@[~a~%~]~{~{~{~{~a ~}~^~%~8TON ~{~{~{~a.~a~} = ~{~a.~a~}~}~^~%AND ~}~}~^~%~1T~} ~^~%CROSS JOIN ~}"
		       " ~@[~%~5TWHERE ~a~]"
		       " ~@[~%GROUP BY ~a~]"
		       "~@[~%HAVING ~a~]"
		       "~@[~%ORDER BY ~a~]"
		       "~@[~%LIMIT ~a~]"
		       "~@[~%OFFSET ~a~]")
	  (hash-table-plist
	   (expression-aliases-of context))
	  (previous-context-of context)
	  (list-from-clause context)
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

    
    
