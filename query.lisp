(in-package #:cl-db)

(defun compute-select-clause (select-item &rest select-list)
  (multiple-value-bind (query loaders)
      (when (not (null select-list))
	(apply #'compute-select-clause select-list))
    (values
     (query-append query
		   :select-list (select-list-of select-item)
		   :from-clause (from-clause-of select-item)
		   :group-by-clause (group-by-clause-of select-item))
     (list* (loader select-item) loaders))))

(defun append-where-clause (query &optional expression
			    &rest rest-clause)
  (if (not (null expression))
      (query-append (apply #'append-where-clause query rest-clause)
		    :where-clause (expression-of expression)
		    :from-clause (from-clause-of expression))
      query))

(defun append-having-clause (query &optional expression
			     &rest rest-clause)
  (if (not (null expression))
      (query-append (apply #'append-where-clause query rest-clause)
		    :having-clause (expression-of expression)
		    :from-clause (from-clause-of expression))
      query))

(defun append-fetch-expressions (query loader fetch-expressions
				 &rest rest-expressions)
  (multiple-value-bind (query loader)
      (if (not (null rest-expressions))
	  (apply #'append-fetch-expressions
		 query rest-expressions)
	  (values query loader))
    (apply #'compute-fetch query loader fetch-expressions)))

(defun wrap-query (query)
  (multiple-value-bind (query-select-list
			query-from-clause
			query-where-clause
			query-group-by-clause
			query-having-clause
			query-order-by
			query-limit
			query-offset)
      (funcall query)
    (let* ((query-alias "main")
	   (select-list
	    (reduce #'(lambda (result select-list-item)
			(let ((alias (rest select-list-item)))
			  (acons (list #'write-column
				       alias query-alias)
				 alias
				 result)))
		    query-select-list
		    :initial-value nil)))
      #'(lambda (&optional column-expression)
	  (if (not (null column-expression))
	      (rassoc column-expression select-list)
	      (values select-list
		      (list
		       (list #'write-subquery
			     (make-query-expression query-select-list
						    query-from-clause
						    query-where-clause
						    query-group-by-clause
						    query-having-clause
						    nil
						    query-limit
						    query-offset)
			     query-alias))
		      nil
		      nil
		      nil
		      query-order-by))))))

(defun append-fetch-clause (query loaders limit offset
			    fetch-expressions)
  (if (not (null fetch-expressions))
      (apply #'append-fetch-expressions
	     (if (not (null (or limit offset)))
		 (wrap-query query)
		 query)
	     (reduce #'(lambda (result loader)
			 (list* loader
				(mapcar #'first
					(remove loader
						fetch-expressions
						:test-not #'eq
						:key #'rest))
				result))
		     loaders :initial-value nil))
      (values query loaders)))

(defun append-order-by-clause (query &optional order-by-expression
			       &rest order-by-clause)
  (if (not (null order-by-expression))
      (query-append (apply #'append-order-by-clause
			   query order-by-clause)
		    :order-by-clause
		    (funcall order-by-expression query))
      query))

(defun make-query-expression (select-list from-clause where-clause
			      group-by-clause having-clause
			      order-by-clause limit offset)
  (remove nil
	  (list
	   (list* #'write-select-list
		  (reduce #'(lambda (result select-list-item)
			      (list* (list #'write-label
					   (first select-list-item)
					   (rest select-list-item))
				     result))
			  select-list :initial-value nil))
	   (list* #'write-from-clause from-clause)
	   (when (not (null where-clause))
	     (list* #'write-where-clause where-clause))
	   (when (some #'null group-by-clause)
	     (list* #'write-group-by-clause
		    (reduce #'append group-by-clause)))
	   (when (not (null having-clause))
	     (list* #'write-having-clause having-clause))
	   (when (not (null order-by-clause))
	     (list* #'write-order-by-clause order-by-clause))
	   (when (not (null limit))
	     (list #'write-limit limit))
	   (when (not (null offset))
	     (list #'write-offset offset)))))

(defun compute-query (select-list where-clause order-by-clause
		      having-clause fetch-clause limit offset)
  (multiple-value-bind (query loaders)
      (apply #'compute-select-clause select-list)
    (let ((query
	   (query-append (apply #'append-having-clause
				(apply #'append-where-clause
				       query where-clause)
				having-clause)
			 :limit limit :offset offset))
	  (fetch-expressions
	   (reduce #'(lambda (result fetch-expr3ession)
		       (multiple-value-call #'acons
			 (funcall fetch-expression) result))
		   fetch-clause :initial-value nil)))
      (multiple-value-bind (query loaders)
	  (append-fetch-clause query loaders limit offset
			       fetch-expressions)
	(multiple-value-bind (sql-string parameters)
	    (make-sql-string
	     (multiple-value-call #'make-query-expression
	       (funcall
		(apply #'append-order-by-clause
		       query order-by-clause))))
	  (values sql-string parameters loaders))))))
